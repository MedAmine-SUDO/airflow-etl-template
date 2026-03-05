"""
s3_hook.py
----------
Thin wrapper around Airflow's built-in S3Hook.
Adds convenience methods for reading and writing JSON/CSV files used in ETL pipelines.
"""

import csv
import json
import io
from airflow.providers.amazon.aws.hooks.s3 import S3Hook as _S3Hook


class S3Hook(_S3Hook):
    """
    Extended S3Hook with helpers for common ETL read/write patterns.
    Credentials are resolved from the Airflow connection (backed by AWS Secrets Manager).
    """

    def read_json(self, bucket: str, key: str) -> list | dict:
        """
        Downloads and parses a JSON file from S3.

        Args:
            bucket: S3 bucket name.
            key:    S3 object key.

        Returns:
            Parsed JSON content (list or dict).
        """
        self.log.info(f"Reading s3://{bucket}/{key}")
        obj = self.get_key(key, bucket_name=bucket)
        return json.loads(obj.get()["Body"].read().decode("utf-8"))

    def read_csv(self, bucket: str, key: str) -> list[dict]:
        """
        Downloads and parses a CSV file from S3.

        Args:
            bucket: S3 bucket name.
            key:    S3 object key.

        Returns:
            List of row dicts (DictReader format).
        """
        self.log.info(f"Reading s3://{bucket}/{key}")
        obj = self.get_key(key, bucket_name=bucket)
        content = obj.get()["Body"].read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))
        return list(reader)

    def write_json(self, data: list | dict, bucket: str, key: str) -> None:
        """
        Serializes data to JSON and uploads to S3.

        Args:
            data:   Data to serialize.
            bucket: S3 bucket name.
            key:    S3 object key.
        """
        self.log.info(f"Writing {len(data) if isinstance(data, list) else 1} records to s3://{bucket}/{key}")
        self.load_string(
            string_data=json.dumps(data, indent=2, default=str),
            key=key,
            bucket_name=bucket,
            replace=True,
        )