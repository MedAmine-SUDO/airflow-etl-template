"""
crm_to_s3_pipeline.py
---------------------
Example ETL pipeline: pulls contact records from a CRM API,
transforms them into a clean analytics-ready format, and loads to S3.

Flow:
    extract_crm_contacts
            |
    transform_contacts
            |
    load_to_s3
            |
    notify_success

Each task is independent — a failure in one does not corrupt the others.
Retries are handled automatically via base_dag defaults.
"""

from datetime import datetime

from airflow.decorators import task
from airflow.utils.dates import days_ago

from base_dag import create_dag
from plugins.hooks.crm_hook import CRMHook
from plugins.hooks.s3_hook import S3Hook


# ── DAG definition ────────────────────────────────────────────────────────────

dag = create_dag(
    dag_id="crm_to_s3_pipeline",
    schedule_interval="0 3 * * *",       # runs daily at 3am UTC
    start_date=days_ago(1),
    description="Extract CRM contacts, transform, and load to S3 as analytics-ready JSON.",
    tags=["crm", "s3", "etl", "example"],
)

# ── Config (loaded from Airflow Variables or .env in local dev) ───────────────

S3_BUCKET = "your-data-bucket"
S3_OUTPUT_KEY = "processed/crm/contacts/{{ ds }}/contacts.json"   # ds = execution date


# ── Tasks ─────────────────────────────────────────────────────────────────────

@task(dag=dag)
def extract_crm_contacts() -> list:
    """
    Pulls all contact records from the CRM API.
    Uses CRMHook which reads credentials from Airflow Connections.
    """
    hook = CRMHook(conn_id="crm_api_default")
    contacts = hook.fetch_records(endpoint="/v1/contacts")
    return contacts


@task(dag=dag)
def transform_contacts(raw_contacts: list) -> list:
    """
    Cleans and normalises raw CRM records into a flat analytics-ready schema.

    - Lowercases email addresses
    - Fills missing fields with None (avoids downstream KeyErrors)
    - Renames fields to a consistent snake_case schema
    - Filters out contacts with no email (not useful for analytics)
    """
    transformed = []

    for record in raw_contacts:
        email = (record.get("email") or "").strip().lower()

        if not email:
            continue  # skip contacts with no email

        transformed.append({
            "contact_id":   record.get("id"),
            "email":        email,
            "first_name":   (record.get("firstName") or record.get("first_name") or "").strip(),
            "last_name":    (record.get("lastName")  or record.get("last_name")  or "").strip(),
            "company":      record.get("company"),
            "created_at":   record.get("createdAt")  or record.get("created_at"),
            "updated_at":   record.get("updatedAt")  or record.get("updated_at"),
            "source":       record.get("source"),
            "tags":         record.get("tags") or [],
        })

    return transformed


@task(dag=dag)
def load_to_s3(contacts: list, s3_key: str = S3_OUTPUT_KEY) -> str:
    """
    Writes the transformed contacts JSON to S3.
    Uses write_json from our S3Hook wrapper.
    Returns the full S3 path for logging.
    """
    hook = S3Hook(aws_conn_id="aws_default")
    hook.write_json(data=contacts, bucket=S3_BUCKET, key=s3_key)

    s3_path = f"s3://{S3_BUCKET}/{s3_key}"
    return s3_path


@task(dag=dag)
def notify_success(s3_path: str, record_count: int) -> None:
    """
    Logs a success summary.
    In production this posts to Slack via SlackWebhookOperator.
    """
    print(f"✅ Pipeline complete — {record_count} contacts written to {s3_path}")


# ── Wire up the pipeline ──────────────────────────────────────────────────────

with dag:
    raw      = extract_crm_contacts()
    clean    = transform_contacts(raw)
    s3_path  = load_to_s3(clean)
    notify_success(s3_path, len(clean) if isinstance(clean, list) else 0)