from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline.healthcare_etl import decrypt_verify_and_export_csv, encrypt_csv_to_parquet, validate_source


with DAG(
    dag_id="healthcare_csv_parquet_export",
    start_date=datetime(2026, 4, 4),
    schedule=None,
    catchup=False,
    tags=["healthcare", "local", "etl"],
) as dag:
    validate_source_task = PythonOperator(
        task_id="validate_source_csv",
        python_callable=validate_source,
    )

    encrypt_to_parquet_task = PythonOperator(
        task_id="encrypt_csv_to_parquet",
        python_callable=encrypt_csv_to_parquet,
    )

    decrypt_verify_export_task = PythonOperator(
        task_id="decrypt_verify_and_export_csv",
        python_callable=decrypt_verify_and_export_csv,
    )

    validate_source_task >> encrypt_to_parquet_task >> decrypt_verify_export_task
