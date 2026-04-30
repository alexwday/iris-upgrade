"""Unit tests for stage_5_database reference filename handling."""

from doc_refresh.stages import stage_5_database


def test_reference_file_name_converts_docx_to_pdf():
    assert stage_5_database._reference_file_name("memo.docx") == "memo.pdf"


def test_reference_file_name_preserves_non_docx_extensions():
    assert stage_5_database._reference_file_name("policy.pdf") == "policy.pdf"
    assert stage_5_database._reference_file_name("sheet.xlsx") == "sheet.xlsx"
