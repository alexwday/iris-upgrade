"""
Stage 5: Database - Sync Database with Processed Documents.

This stage performs database operations using the 2-table design:
- iris_document_metadata: Document-level metadata with summary
- iris_document_chunks: Chunk-level content with embeddings

Operations:
- Remove deleted/updated files from database
- Insert new/updated documents with chunks
- All operations in transactions for atomicity

Note: This pipeline is designed for single-instance execution per
db_source. Concurrent runs against the same db_source are not
supported and may cause data inconsistency.

Functions:
    run_stage: Execute the database sync stage
    remove_document: Remove a document and its chunks from database
    insert_document: Insert a document with all chunks
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..connections.postgres import get_database_session
from ..stages.stage_4_validate import ValidatedDocument

logger = logging.getLogger(__name__)


# Table names for the 2-table design
METADATA_TABLE = "iris_document_metadata"
CHUNKS_TABLE = "iris_document_chunks"


@dataclass
class DatabaseResult:
    """Result of the database sync stage."""

    documents_removed: int = 0
    documents_inserted: int = 0
    sections_inserted: int = 0
    chunks_inserted: int = 0
    errors: List[str] = field(default_factory=list)


def _reference_file_name(source_file_name: str) -> str:
    """Return the filename stored for viewer/S3 references."""
    if source_file_name.lower().endswith(".docx"):
        return str(Path(source_file_name).with_suffix(".pdf"))
    return source_file_name


def run_stage(
    files_to_remove: List[Dict],
    validated_documents: List[ValidatedDocument],
    dry_run: bool = False,
    force: bool = False,
) -> DatabaseResult:
    """
    Execute the database sync stage.

    Removes deleted files and inserts new/updated documents.
    When force=True, performs a bulk DELETE by db_source before inserting.

    All database mutations for a given logical unit (force-mode per
    db_source, or per-document remove+insert) happen within a single
    transaction to prevent data loss on crash.

    Args:
        files_to_remove: List of dicts with 'db_source', 'file_path' to remove.
        validated_documents: List of ValidatedDocument from Stage 4.
        dry_run: If True, don't actually modify database.
        force: If True, bulk-delete all documents for each db_source before insert.

    Returns:
        DatabaseResult with operation counts and any errors.
    """
    result = DatabaseResult()

    if dry_run:
        logger.info("DRY RUN: Database operations will be simulated")

    if force and validated_documents:
        db_sources = {v.document.file_info.db_source for v in validated_documents}
        for db_source in db_sources:
            source_docs = [
                v for v in validated_documents
                if v.document.file_info.db_source == db_source
            ]
            try:
                if not dry_run:
                    removed, inserted, sections, chunks = (
                        _force_replace_db_source(db_source, source_docs)
                    )
                    result.documents_removed += removed
                    result.documents_inserted += inserted
                    result.sections_inserted += sections
                    result.chunks_inserted += chunks
                else:
                    logger.info(
                        "DRY RUN: Would bulk-delete all documents for db_source=%s "
                        "and insert %d documents",
                        db_source,
                        len(source_docs),
                    )
                    for validated in source_docs:
                        doc = validated.document
                        result.documents_inserted += 1
                        result.sections_inserted += len(doc.sections)
                        result.chunks_inserted += len(doc.chunks)
            except Exception as exc:
                error_msg = f"Failed force replace for {db_source}: {exc}"
                logger.error(error_msg)
                result.errors.append(error_msg)

    if not force and files_to_remove:
        logger.info("Removing %d documents from database", len(files_to_remove))
        for file_info in files_to_remove:
            doc_path = file_info.get("file_path", "")
            try:
                if not dry_run:
                    removed = remove_document(
                        file_info["db_source"], doc_path
                    )
                    if removed:
                        result.documents_removed += 1
                else:
                    logger.info(
                        "DRY RUN: Would remove %s/%s",
                        file_info["db_source"],
                        doc_path,
                    )
                    result.documents_removed += 1
            except Exception as exc:
                error_msg = f"Failed to remove {doc_path}: {exc}"
                logger.error(error_msg)
                result.errors.append(error_msg)

    if validated_documents and not force:
        logger.info("Inserting %d validated documents", len(validated_documents))
        for validated in validated_documents:
            doc = validated.document
            try:
                if not dry_run:
                    sections, chunks = replace_single_document(doc)
                    result.documents_inserted += 1
                    result.sections_inserted += sections
                    result.chunks_inserted += chunks
                else:
                    logger.info(
                        "DRY RUN: Would insert %s (%d sections, %d chunks)",
                        doc.file_info.file_name,
                        len(doc.sections),
                        len(doc.chunks),
                    )
                    result.documents_inserted += 1
                    result.sections_inserted += len(doc.sections)
                    result.chunks_inserted += len(doc.chunks)

            except Exception as exc:
                error_msg = f"Failed to insert {doc.file_info.file_name}: {exc}"
                logger.error(error_msg)
                result.errors.append(error_msg)

    # Log summary
    logger.info(
        "Database sync complete: %d removed, %d inserted (%d sections, %d chunks), %d errors",
        result.documents_removed,
        result.documents_inserted,
        result.sections_inserted,
        result.chunks_inserted,
        len(result.errors),
    )

    return result


def _force_replace_db_source(
    db_source: str,
    validated_docs: List[ValidatedDocument],
) -> Tuple[int, int, int, int]:
    """Bulk-delete and reinsert all documents for a db_source atomically.

    Returns:
        Tuple of (documents_removed, documents_inserted, sections, chunks).
    """
    with get_database_session() as session:
        removed = _bulk_delete_by_source(db_source, session)
        logger.info(
            "Force mode: bulk-deleted %d documents for db_source=%s",
            removed,
            db_source,
        )

        total_inserted = 0
        total_sections = 0
        total_chunks = 0
        for validated in validated_docs:
            doc = validated.document
            sections, chunks = _insert_document_impl(doc, session)
            total_inserted += 1
            total_sections += sections
            total_chunks += chunks

    return removed, total_inserted, total_sections, total_chunks


def replace_single_document(doc: Any) -> Tuple[int, int]:
    """Remove old version and insert new version of a document atomically.

    Returns:
        Tuple of (sections_count, chunks_inserted).
    """
    with get_database_session() as session:
        _remove_document_impl(
            doc.file_info.db_source, doc.file_info.relative_path, session
        )
        return _insert_document_impl(doc, session)


def _bulk_delete_by_source(db_source: str, session: Session) -> int:
    """Delete all documents and their chunks for a db_source within a session."""
    delete_chunks_query = text(
        f"""
        DELETE FROM {CHUNKS_TABLE}
        WHERE document_id IN (
            SELECT id FROM {METADATA_TABLE} WHERE db_source = :db_source
        )
        """
    )
    delete_docs_query = text(
        f"DELETE FROM {METADATA_TABLE} WHERE db_source = :db_source"
    )

    session.execute(delete_chunks_query, {"db_source": db_source})
    result = session.execute(delete_docs_query, {"db_source": db_source})
    removed = result.rowcount if result else 0

    logger.info("Bulk-deleted %d documents for db_source=%s", removed, db_source)
    return removed


def remove_document(
    db_source: str, file_path: str, session: Optional[Session] = None
) -> bool:
    """
    Remove a document and all related chunks from database.

    Args:
        db_source: Database source identifier.
        file_path: Relative file path used as document identity.
        session: Optional existing session (creates own transaction if None).

    Returns:
        True if document was removed, False if not found.
    """
    if session is not None:
        return _remove_document_impl(db_source, file_path, session)

    with get_database_session() as new_session:
        return _remove_document_impl(db_source, file_path, new_session)


def _remove_document_impl(
    db_source: str, file_path: str, session: Session
) -> bool:
    """Remove a document and its chunks within an existing session."""
    find_query = text(
        f"""
        SELECT id FROM {METADATA_TABLE}
        WHERE db_source = :db_source AND file_path = :file_path
        """
    )
    delete_chunks = text(
        f"DELETE FROM {CHUNKS_TABLE} WHERE document_id = :document_id"
    )
    delete_document = text(
        f"DELETE FROM {METADATA_TABLE} WHERE id = :document_id"
    )

    result = session.execute(
        find_query, {"db_source": db_source, "file_path": file_path}
    ).fetchone()

    if not result:
        logger.debug("Document not found in DB: %s/%s", db_source, file_path)
        return False

    document_id = result[0]

    chunk_result = session.execute(delete_chunks, {"document_id": document_id})
    chunks_removed = chunk_result.rowcount if chunk_result else 0
    session.execute(delete_document, {"document_id": document_id})

    logger.info("Removed document %s: %d chunks", file_path, chunks_removed)
    return True


def insert_document(doc: Any, session: Optional[Session] = None) -> Tuple[int, int]:
    """
    Insert a document with all chunks into the 2-table design.

    Inserts into:
    - iris_document_metadata: Document-level info with summary and embedding
    - iris_document_chunks: Chunk-level content with embeddings

    Args:
        doc: ProcessedDocument to insert.
        session: Optional existing session (creates own transaction if None).

    Returns:
        Tuple of (sections_count, chunks_inserted).
    """
    if session is not None:
        return _insert_document_impl(doc, session)

    with get_database_session() as new_session:
        return _insert_document_impl(doc, new_session)


def _insert_document_impl(doc: Any, session: Session) -> Tuple[int, int]:
    """Insert a document and all its chunks within an existing session."""
    summary_embedding_str = None
    if doc.summary_embedding and len(doc.summary_embedding) > 0:
        summary_embedding_str = "[" + ",".join(
            str(x) for x in doc.summary_embedding
        ) + "]"
    reference_file_name = _reference_file_name(doc.file_info.file_name)

    insert_metadata = text(
        f"""
        INSERT INTO {METADATA_TABLE} (
            db_source, document_name, document_type,
            document_summary, summary_embedding,
            page_count, primary_section_count, subsection_count,
            file_name, file_path, file_size, file_hash, file_type,
            document_description, document_usage
        ) VALUES (
            :db_source, :document_name, :document_type,
            :document_summary, CAST(:summary_embedding AS halfvec),
            :page_count, :primary_section_count, :subsection_count,
            :file_name, :file_path, :file_size, :file_hash, :file_type,
            :document_description, :document_usage
        )
        RETURNING id
        """
    )

    metadata_result = session.execute(
        insert_metadata,
        {
            "db_source": doc.file_info.db_source,
            "document_name": getattr(doc, "document_display_name", "") or doc.file_info.file_name,
            "document_type": doc.structure_type.value,
            "document_summary": doc.document_summary,
            "summary_embedding": summary_embedding_str,
            "page_count": doc.page_count,
            "primary_section_count": doc.primary_section_count,
            "subsection_count": doc.subsection_count,
            "file_name": reference_file_name,
            "file_path": doc.file_info.relative_path,
            "file_size": doc.file_info.file_size,
            "file_hash": doc.file_info.file_hash,
            "file_type": doc.file_info.file_name.rsplit(".", 1)[-1]
            if "." in doc.file_info.file_name
            else None,
            "document_description": doc.document_description,
            "document_usage": doc.document_usage,
        },
    )
    document_id = metadata_result.scalar_one()

    insert_chunk = text(
        f"""
        INSERT INTO {CHUNKS_TABLE} (
            document_id, db_source, chunk_number,
            primary_section_number, primary_section_name,
            subsection_number, subsection_name,
            hierarchy_path,
            chunk_content, chunk_embedding,
            page_number,
            primary_section_page_count, subsection_page_count,
            file_name, source_filename
        ) VALUES (
            :document_id, :db_source, :chunk_number,
            :primary_section_number, :primary_section_name,
            :subsection_number, :subsection_name,
            :hierarchy_path,
            :chunk_content, CAST(:chunk_embedding AS halfvec),
            :page_number,
            :primary_section_page_count, :subsection_page_count,
            :file_name, :source_filename
        )
        """
    )

    chunks_inserted = 0
    for chunk in doc.chunks:
        embedding_str = None
        if chunk.embedding and len(chunk.embedding) > 0:
            embedding_str = "[" + ",".join(str(x) for x in chunk.embedding) + "]"

        session.execute(
            insert_chunk,
            {
                "document_id": document_id,
                "db_source": doc.file_info.db_source,
                "chunk_number": chunk.chunk_number,
                "primary_section_number": chunk.primary_section_number,
                "primary_section_name": chunk.primary_section_name,
                "subsection_number": chunk.subsection_number,
                "subsection_name": chunk.subsection_name,
                "hierarchy_path": chunk.hierarchy_path,
                "chunk_content": chunk.raw_content,
                "chunk_embedding": embedding_str,
                "page_number": chunk.page_number,
                "primary_section_page_count": chunk.primary_section_page_count,
                "subsection_page_count": chunk.subsection_page_count,
                "file_name": reference_file_name,
                "source_filename": reference_file_name,
            },
        )
        chunks_inserted += 1

    logger.info(
        "Inserted document %s: ID=%s, %d sections, %d subsections, %d chunks",
        doc.file_info.file_name,
        document_id,
        doc.primary_section_count,
        doc.subsection_count,
        chunks_inserted,
    )

    return doc.primary_section_count, chunks_inserted
