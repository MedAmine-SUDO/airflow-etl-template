"""
base_dag.py
-----------
Shared defaults and utilities inherited by all DAGs in this project.
Centralizes retry logic, alerting, and owner config in one place.
"""

from datetime import datetime, timedelta
from airflow.models import DAG


def get_default_args(owner: str = "data-team") -> dict:
    """
    Returns a standard set of DAG default arguments.
    All DAGs in this project inherit these unless explicitly overridden.
    """
    return {
        "owner": owner,
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(hours=1),
    }


def create_dag(
    dag_id: str,
    schedule_interval: str,
    start_date: datetime,
    description: str = "",
    tags: list = None,
    catchup: bool = False,
) -> DAG:
    """
    Factory function to create a DAG with consistent settings.

    Args:
        dag_id:            Unique identifier for the DAG.
        schedule_interval: Cron expression or preset (e.g. "@daily").
        start_date:        DAG start date.
        description:       Short description shown in the Airflow UI.
        tags:              List of tags for filtering in the UI.
        catchup:           Whether to backfill missed runs (default: False).

    Returns:
        A configured Airflow DAG instance.
    """
    return DAG(
        dag_id=dag_id,
        default_args=get_default_args(),
        description=description,
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=catchup,
        tags=tags or [],
        max_active_runs=1,
    )