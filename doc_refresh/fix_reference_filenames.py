"""
Reference Filename Repair - Update DOCX references to PDF filenames.

This maintenance script fixes existing IRIS document rows after DOCX source
files have been converted to PDF in the output/S3 publishing flow. It updates
stored filename fields from .docx to .pdf without reprocessing documents, while
preserving file_path and file_hash for refresh identity and change detection.

By default, the script uses the same environment-driven settings as
doc_refresh: PostgreSQL connection variables, DATABASE_NAMES for optional source
filtering, FILE_SOURCE_MODE for backup storage, and BACKUP_PATH for before/after
CSV backups. Use CLI flags only when you need to override the current env.
"""

import argparse
import logging
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from sqlalchemy import bindparam, text

from .connections.file_source import FileSource, get_file_source
from .connections.postgres import get_database_session
from .utils import backup
from .utils.env_config import config
from .utils.logging_format import configure_root_logger

logger = logging.getLogger(__name__)


@dataclass
class RepairCounts:
    """Counts of rows needing repair and rows updated."""

    metadata_to_fix: int = 0
    chunks_to_fix: int = 0
    metadata_updated: int = 0
    chunks_updated: int = 0

    @property
    def has_changes(self) -> bool:
        """Return True when at least one row was updated."""
        return self.metadata_updated > 0 or self.chunks_updated > 0


def _parse_csv(raw: str) -> List[str]:
    """Split comma-separated values into trimmed non-empty tokens."""
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


def _parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fix existing DOCX reference filenames to point at PDF outputs.",
    )
    parser.add_argument(
        "--db-sources",
        default=config.DATABASE_NAMES,
        help=(
            "Comma-separated db_source values to repair. Defaults to DATABASE_NAMES "
            "from the current env; empty means all db_sources."
        ),
    )
    parser.add_argument(
        "--backup-path",
        default=config.BACKUP_PATH,
        help=(
            "Backup root for before/after CSV backups. Defaults to BACKUP_PATH "
            "from the current env."
        ),
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Apply updates without writing before/after CSV backups.",
    )
    parser.add_argument(
        "--no-it-upload-export",
        action="store_true",
        help="Skip writing the portable IT upload CSV export bundle after repair.",
    )
    parser.add_argument(
        "--it-upload-export-path",
        default="",
        help=(
            "Optional destination for the portable export bundle. Defaults to "
            "<BACKUP_PATH>/backup_<timestamp>/it_upload_export."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report rows that would be updated without changing the database.",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=config.LOG_LEVEL,
        help="Logging level.",
    )
    return parser.parse_args()


def _source_filter_sql(db_sources: List[str]) -> str:
    """Return SQL condition for optional db_source filtering."""
    if not db_sources:
        return ""
    return " AND db_source IN :db_sources"


def _prepare_query(sql: str, db_sources: List[str]):
    """Bind expanding db_sources parameter when a source filter is active."""
    query = text(sql)
    if db_sources:
        query = query.bindparams(bindparam("db_sources", expanding=True))
    return query


def _query_params(db_sources: List[str]) -> dict:
    """Return SQLAlchemy parameters for optional db_source filtering."""
    if not db_sources:
        return {}
    return {"db_sources": db_sources}


def _count_rows_to_fix(db_sources: List[str]) -> RepairCounts:
    """Count metadata and chunk rows whose filename fields end in .docx."""
    source_filter = _source_filter_sql(db_sources)
    metadata_query = _prepare_query(
        f"""
        SELECT COUNT(*)
        FROM iris_document_metadata
        WHERE file_name ~* '\\.docx$'
        {source_filter}
        """,
        db_sources,
    )
    chunks_query = _prepare_query(
        f"""
        SELECT COUNT(*)
        FROM iris_document_chunks
        WHERE (
            file_name ~* '\\.docx$'
            OR source_filename ~* '\\.docx$'
        )
        {source_filter}
        """,
        db_sources,
    )

    with get_database_session() as session:
        params = _query_params(db_sources)
        metadata_count = session.execute(metadata_query, params).scalar_one()
        chunk_count = session.execute(chunks_query, params).scalar_one()

    return RepairCounts(
        metadata_to_fix=int(metadata_count or 0),
        chunks_to_fix=int(chunk_count or 0),
    )


def _apply_repair(db_sources: List[str]) -> RepairCounts:
    """Update .docx filename fields to .pdf in metadata and chunk rows."""
    source_filter = _source_filter_sql(db_sources)
    metadata_update = _prepare_query(
        f"""
        UPDATE iris_document_metadata
        SET
            file_name = regexp_replace(file_name, '\\.docx$', '.pdf', 'i'),
            updated_at = CURRENT_TIMESTAMP
        WHERE file_name ~* '\\.docx$'
        {source_filter}
        """,
        db_sources,
    )
    chunks_update = _prepare_query(
        f"""
        UPDATE iris_document_chunks
        SET
            file_name = CASE
                WHEN file_name ~* '\\.docx$'
                THEN regexp_replace(file_name, '\\.docx$', '.pdf', 'i')
                ELSE file_name
            END,
            source_filename = CASE
                WHEN source_filename ~* '\\.docx$'
                THEN regexp_replace(source_filename, '\\.docx$', '.pdf', 'i')
                ELSE source_filename
            END
        WHERE (
            file_name ~* '\\.docx$'
            OR source_filename ~* '\\.docx$'
        )
        {source_filter}
        """,
        db_sources,
    )

    with get_database_session() as session:
        params = _query_params(db_sources)
        metadata_result = session.execute(metadata_update, params)
        chunks_result = session.execute(chunks_update, params)

    return RepairCounts(
        metadata_updated=metadata_result.rowcount or 0,
        chunks_updated=chunks_result.rowcount or 0,
    )


def _backup_file_source() -> Optional[FileSource]:
    """Return NAS file source for NAS backups, otherwise local filesystem writes."""
    if config.FILE_SOURCE_MODE.lower() == "nas":
        return get_file_source()
    return None


def _join_storage_path(base: str, relative: str) -> str:
    """Join local or NAS-style paths using forward slashes."""
    base_clean = base.replace("\\", "/").rstrip("/")
    rel_clean = relative.replace("\\", "/").strip("/")
    if base_clean and rel_clean:
        return f"{base_clean}/{rel_clean}"
    return base_clean or rel_clean


def _run_backup(
    backup_path: str,
    backup_stamp: str,
    backup_phase: str,
    file_source: Optional[FileSource],
) -> None:
    """Run one CSV backup phase and raise if it fails."""
    success, files = backup.run_backup(
        backup_path,
        file_source=file_source,
        backup_stamp=backup_stamp,
        backup_phase=backup_phase,
    )
    if not success:
        raise RuntimeError(f"{backup_phase} backup failed")
    logger.info("%s backup files: %s", backup_phase, files)


def _copy_local_tree_to_file_source(
    local_root: Path,
    destination_root: str,
    file_source: FileSource,
) -> None:
    """Copy all files under a local folder to a FileSource destination."""
    for path in sorted(local_root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(local_root).as_posix()
        destination = _join_storage_path(destination_root, relative)
        file_source.copy_from_local(str(path), destination)


def _run_it_upload_export(
    destination_root: str,
    file_source: Optional[FileSource],
) -> None:
    """Run the existing portable exporter and place output at destination_root."""
    project_root = Path(__file__).resolve().parents[1]
    exporter = project_root / "db_config" / "export_portable_iris_data.py"

    if file_source is None:
        output_dir = Path(destination_root)
        output_dir.mkdir(parents=True, exist_ok=True)
        command = [sys.executable, str(exporter), "--output-dir", str(output_dir)]
        logger.info("Writing IT upload export: %s", output_dir)
        subprocess.run(command, check=True, cwd=str(project_root))
        return

    with tempfile.TemporaryDirectory(prefix="iris_it_upload_export_") as tmp_dir:
        local_output = Path(tmp_dir) / "it_upload_export"
        command = [sys.executable, str(exporter), "--output-dir", str(local_output)]
        logger.info("Writing temporary IT upload export: %s", local_output)
        subprocess.run(command, check=True, cwd=str(project_root))
        logger.info("Copying IT upload export to: %s", destination_root)
        file_source.ensure_directory(destination_root)
        _copy_local_tree_to_file_source(local_output, destination_root, file_source)


def main() -> int:
    """Run the reference filename repair."""
    args = _parse_args()
    configure_root_logger(getattr(logging, args.log_level.upper(), logging.INFO))

    db_sources = _parse_csv(args.db_sources)
    scope = ", ".join(db_sources) if db_sources else "ALL"
    logger.info("Reference filename repair starting")
    logger.info("  DB sources: %s", scope)
    logger.info("  Dry run: %s", args.dry_run)
    logger.info("  Backup path: %s", args.backup_path or "<none>")
    logger.info("  IT upload export: %s", not args.no_it_upload_export)

    before_counts = _count_rows_to_fix(db_sources)
    logger.info(
        "Rows needing repair: metadata=%d chunks=%d",
        before_counts.metadata_to_fix,
        before_counts.chunks_to_fix,
    )

    if before_counts.metadata_to_fix == 0 and before_counts.chunks_to_fix == 0:
        logger.info("No .docx reference filename rows found. Nothing to repair.")
        return 0

    if args.dry_run:
        logger.info("DRY RUN: no database updates or backups written")
        return 0

    if (
        (not args.no_backup or not args.no_it_upload_export)
        and not args.backup_path
        and not args.it_upload_export_path
    ):
        logger.error(
            "Set BACKUP_PATH/--backup-path, pass --it-upload-export-path, "
            "or disable backups/exports"
        )
        return 1

    file_source = None
    backup_stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    try:
        if not args.no_backup or not args.no_it_upload_export:
            file_source = _backup_file_source()

        if not args.no_backup:
            _run_backup(
                args.backup_path,
                backup_stamp,
                "before_reference_filename_fix",
                file_source,
            )

        updated_counts = _apply_repair(db_sources)
        logger.info(
            "Rows updated: metadata=%d chunks=%d",
            updated_counts.metadata_updated,
            updated_counts.chunks_updated,
        )

        if not args.no_backup:
            _run_backup(
                args.backup_path,
                backup_stamp,
                "after_reference_filename_fix",
                file_source,
            )

        if not args.no_it_upload_export:
            export_destination = args.it_upload_export_path.strip() or _join_storage_path(
                args.backup_path,
                f"backup_{backup_stamp}/it_upload_export",
            )
            _run_it_upload_export(export_destination, file_source)
    finally:
        if hasattr(file_source, "close"):
            file_source.close()

    remaining_counts = _count_rows_to_fix(db_sources)
    logger.info(
        "Rows still needing repair: metadata=%d chunks=%d",
        remaining_counts.metadata_to_fix,
        remaining_counts.chunks_to_fix,
    )

    if remaining_counts.metadata_to_fix or remaining_counts.chunks_to_fix:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
