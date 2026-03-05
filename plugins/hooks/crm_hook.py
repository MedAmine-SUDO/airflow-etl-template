"""
crm_hook.py
-----------
Custom Airflow hook for authenticating and fetching data from a REST API (CRM/SaaS).
Credentials are loaded from Airflow Connections (backed by AWS Secrets Manager in prod).
"""

import requests
from airflow.hooks.base import BaseHook


class CRMHook(BaseHook):
    """
    Hook to interact with a generic CRM REST API.

    Expects an Airflow HTTP connection with:
        - host: base URL of the API  (e.g. https://api.yourcrm.com)
        - password: Bearer token or API key
    """

    conn_type = "http"

    def __init__(self, conn_id: str = "crm_api_default"):
        super().__init__()
        self.conn_id = conn_id
        self._session = None

    def get_conn(self) -> requests.Session:
        """
        Returns an authenticated requests Session.
        Reuses the session if already established.
        """
        if self._session:
            return self._session

        connection = self.get_connection(self.conn_id)
        session = requests.Session()
        session.headers.update({
            "Authorization": f"Bearer {connection.password}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })
        self.base_url = connection.host
        self._session = session
        return session

    def fetch_records(self, endpoint: str, params: dict = None) -> list:
        """
        Fetches paginated records from a CRM endpoint.

        Args:
            endpoint: API path (e.g. "/v1/contacts")
            params:   Optional query parameters.

        Returns:
            List of record dicts.
        """
        session = self.get_conn()
        records = []
        page = 1

        while True:
            request_params = {**(params or {}), "page": page, "per_page": 100}
            response = session.get(f"{self.base_url}{endpoint}", params=request_params)
            response.raise_for_status()

            data = response.json()
            batch = data.get("data") or data.get("records") or data.get("results") or []

            if not batch:
                break

            records.extend(batch)
            self.log.info(f"Fetched page {page} — {len(batch)} records from {endpoint}")

            # Stop if we've reached the last page
            if len(batch) < 100:
                break

            page += 1

        self.log.info(f"Total records fetched from {endpoint}: {len(records)}")
        return records