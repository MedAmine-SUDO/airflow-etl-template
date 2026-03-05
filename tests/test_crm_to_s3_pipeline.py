"""
test_crm_to_s3_pipeline.py
--------------------------
Unit tests for the CRM → S3 pipeline transformation logic.
No real API calls or AWS connections are made — all external services are mocked.
"""

import pytest
from unittest.mock import MagicMock, patch
from dags.crm_to_s3_pipeline import transform_contacts


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def raw_contacts():
    return [
        {
            "id": "001",
            "email": "  Alice@Example.com  ",
            "firstName": "Alice",
            "lastName": "Smith",
            "company": "Acme Corp",
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-06-01T00:00:00Z",
            "source": "web",
            "tags": ["lead", "enterprise"],
        },
        {
            "id": "002",
            "email": "",           # should be filtered out
            "firstName": "Bob",
            "lastName": "Jones",
            "company": None,
            "createdAt": "2024-02-01T00:00:00Z",
            "updatedAt": None,
            "source": None,
            "tags": [],
        },
        {
            "id": "003",
            "email": "carol@example.com",
            "first_name": "Carol",  # snake_case variant — should still work
            "last_name": "White",
            "company": "Beta Inc",
            "createdAt": None,
            "updatedAt": None,
            "source": "import",
            "tags": None,           # None tags should become []
        },
    ]


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_transform_filters_contacts_without_email(raw_contacts):
    """Contacts with no email must be excluded from the output."""
    result = transform_contacts(raw_contacts)
    emails = [r["email"] for r in result]
    assert "" not in emails
    assert len(result) == 2


def test_transform_lowercases_email(raw_contacts):
    """Email addresses must be normalised to lowercase."""
    result = transform_contacts(raw_contacts)
    alice = next(r for r in result if r["contact_id"] == "001")
    assert alice["email"] == "alice@example.com"


def test_transform_strips_whitespace_from_email(raw_contacts):
    """Leading/trailing whitespace must be removed from emails."""
    result = transform_contacts(raw_contacts)
    alice = next(r for r in result if r["contact_id"] == "001")
    assert alice["email"] == alice["email"].strip()


def test_transform_handles_snake_case_name_fields(raw_contacts):
    """Both camelCase and snake_case name fields should be handled."""
    result = transform_contacts(raw_contacts)
    carol = next(r for r in result if r["contact_id"] == "003")
    assert carol["first_name"] == "Carol"
    assert carol["last_name"] == "White"


def test_transform_none_tags_become_empty_list(raw_contacts):
    """None tags must be normalised to an empty list."""
    result = transform_contacts(raw_contacts)
    carol = next(r for r in result if r["contact_id"] == "003")
    assert carol["tags"] == []


def test_transform_output_schema(raw_contacts):
    """Every output record must contain the expected keys."""
    expected_keys = {
        "contact_id", "email", "first_name", "last_name",
        "company", "created_at", "updated_at", "source", "tags",
    }
    result = transform_contacts(raw_contacts)
    for record in result:
        assert set(record.keys()) == expected_keys


def test_transform_empty_input():
    """An empty input list must return an empty output list."""
    assert transform_contacts([]) == []