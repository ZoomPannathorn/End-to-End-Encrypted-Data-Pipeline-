# End-to-End Encrypted Data Pipeline

This project demonstrates a local end-to-end data engineering pipeline that ingests a healthcare CSV file, encrypts a sensitive column, stores the result as Parquet, verifies decryption integrity, and exports a restricted downstream dataset. The workflow is orchestrated with Apache Airflow and packaged with Docker Compose for local execution.

## What the Project Does

The pipeline processes a healthcare-style dataset and protects a sensitive financial field during transformation.

Pipeline flow:

1. Validate that the source CSV exists.
2. Encrypt the `Billing Amount` column with RSA.
3. Store the transformed dataset in Parquet format.
4. Read the encrypted Parquet file and decrypt the sensitive values.
5. Verify decrypted values against the original CSV.
6. Export a limited CSV containing only `Name` and `Age`.
7. Generate a verification report in JSON.

## Architecture

```text
Raw CSV
  -> Validation
  -> RSA Encryption of Billing Amount
  -> Encrypted Parquet Output
  -> Decryption + Verification
  -> Restricted CSV Export + Verification Report
```

## Airflow DAG

The orchestration DAG is `healthcare_csv_parquet_export` and runs these tasks:

- `validate_source_csv`
- `encrypt_csv_to_parquet`
- `decrypt_verify_and_export_csv`

## Tech Stack

- Python
- Apache Airflow
- Docker Compose
- PyArrow
- Cryptography
- Parquet

## Repository Structure

```text
.
+-- dags/
|   \-- healthcare_pipeline_dag.py
+-- pipeline/
|   \-- healthcare_etl.py
+-- docker-compose.yml
+-- Dockerfile.airflow
+-- healthcare_dataset.csv
\-- requirements-airflow.txt
```

## Local Run

Start Airflow:

```powershell
docker compose up --build airflow-init
docker compose up --build
```

Open Airflow at:

`http://localhost:8081`

Login:

- Username: `admin`
- Password: `admin`

Then trigger the DAG `healthcare_csv_parquet_export`.

## Output Artifacts

Generated locally during runtime:

- `processed_parquet/healthcare_encrypted.parquet`
- `exports/name_age_export.csv`
- `exports/verification_report.json`

These generated files are intentionally excluded from GitHub.

## Security Note

This repository does not include generated RSA key files. The pipeline creates key material locally at runtime if keys do not already exist. This keeps the repository safer and avoids publishing private cryptographic assets.

## Resume Value

This project demonstrates:

- orchestration of a multi-step ETL workflow with Airflow
- selective encryption of sensitive data fields
- Parquet-based storage for efficient downstream processing
- data verification before publishing outputs
- reproducible local execution with Docker

