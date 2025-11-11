from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import os

API_KEY = os.getenv("FLIGHT_API_KEY")

default_args = {"owner": "airflow", "start_date": datetime(2025, 1, 1)}

BUCKET_NAME = "flight-intel-raw"
def _fetch_flights(**context):
    http = HttpHook(http_conn_id="aviationstack_api", method="GET")
    endpoint = "flights"
    params = {"access_key": API_KEY, "limit": 10}
    # IMPORTANT: for HttpHook GET, pass query params via `data=...`
    resp = http.run(endpoint=endpoint, data=params)
    data = resp.json()
    context["ti"].xcom_push(key="flights_data", value=data)
    return data


def _upload_to_gcs(**context):
    ti = context["ti"]
    data = ti.xcom_pull(task_ids="fetch_flights", key="flights_data")
    filename = f"flights_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    local_path = f"/opt/airflow/logs/{filename}"

    # Save locally
    with open(local_path, "w") as f:
        json.dump(data, f)

    # Upload to GCS
    gcs = GCSHook(gcp_conn_id="google_cloud_default")
    gcs.upload(BUCKET_NAME, filename, local_path)

with DAG(
    "ingest_flight_status",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    fetch = PythonOperator(
        task_id="fetch_flights",
        python_callable=_fetch_flights,
        provide_context=True,
    )

    upload = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=_upload_to_gcs,
        provide_context=True,
    )
    
    load_bq = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=["flights_*.json"],  # wildcard matches all uploads
        destination_project_dataset_table="flight-intel-platform.flights_dataset.flights_raw",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
        write_disposition="WRITE_APPEND",
        gcp_conn_id="google_cloud_default",
    )


    fetch >> upload >> load_bq
