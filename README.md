# Airflow ETL Template

Production-ready Apache Airflow pipeline template for extracting data from REST APIs and AWS S3, transforming it into analytics-ready datasets, and loading it to S3.

Built by [Mohamed Amine Ben Afia](https://linkedin.com/in/mabenafia) — Lead Python Developer with 4+ years building data infrastructure at scale.

---

## What This Template Demonstrates

- Clean DAG structure with task-level independence
- Reusable hooks for CRM APIs and S3
- Centralised credentials via Airflow Connections (AWS Secrets Manager in prod)
- Pytest test suite covering transformation logic
- Local dev environment via Docker Compose (mirrors MWAA production setup)
- Retry logic, timeouts, and execution guardrails out of the box

---

## Pipeline: CRM → S3

```
extract_crm_contacts
        │
transform_contacts       ← normalise, validate, filter
        │
load_to_s3               ← partitioned by execution date
        │
notify_success
```

Each task is independent. A failure at any stage retries automatically without re-running upstream tasks.

---

## Project Structure

```
airflow-etl-template/
├── dags/
│   ├── base_dag.py               # Shared DAG factory & default args
│   └── crm_to_s3_pipeline.py     # Example pipeline DAG
├── plugins/
│   └── hooks/
│       ├── crm_hook.py           # Paginated REST API hook
│       └── s3_hook.py            # S3 read/write helpers
├── tests/
│   └── test_crm_to_s3_pipeline.py
├── config/                       # Local connections for dev (gitignored in prod)
├── docker-compose.yml            # Local Airflow environment
├── .env.example                  # Environment variable template
└── requirements.txt
```

---

## Getting Started

### 1. Clone and configure

```bash
git clone https://github.com/yourusername/airflow-etl-template.git
cd airflow-etl-template
cp .env.example .env
# Fill in your CRM API token and AWS credentials in .env
```

### 2. Start the local environment

```bash
docker-compose up airflow-init
docker-compose up
```

Airflow UI will be available at **http://localhost:8080** (admin / admin).

### 3. Run the tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

---

## Credentials & Security

Locally, credentials are loaded from `.env` and `config/connections.json`.

In production (MWAA), all credentials are stored in **AWS Secrets Manager** and injected automatically into Airflow Connections — no secrets in code or environment variables.

```
AWS Secrets Manager
      │
      └── airflow/connections/crm_api_default   → CRM API token
      └── airflow/connections/aws_default        → S3 / AWS access
```

---

## Adapting for Your Use Case

| What you want to change | Where to change it |
|---|---|
| API endpoint / auth method | `plugins/hooks/crm_hook.py` |
| Transformation logic | `transform_contacts()` in the DAG |
| Output format / destination | `load_to_s3()` in the DAG + `s3_hook.py` |
| Schedule | `schedule_interval` in `create_dag()` call |
| Retry behaviour | `get_default_args()` in `base_dag.py` |

---

## Production Notes

This template is based on patterns used in production at a French EdTech platform serving hundreds of enterprise clients:

- Deployed on **AWS MWAA** (Managed Workflows for Apache Airflow)
- Pipelines process **millions of rows daily** across multiple tenants
- Monitoring via **Datadog + CloudWatch**, alerts via **Slack bot**
- CI/CD via **GitLab CI** with automated test gates on every PR

---

## Contact

Open to data engineering contracts — [LinkedIn](https://www.linkedin.com/in/mabenafia/) · [Upwork](https://www.upwork.com/freelancers/~01de6a80acbbaa49db)