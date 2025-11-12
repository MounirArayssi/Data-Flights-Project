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
    import requests, time, json

    url = "https://opensky-network.org/api/states/all"
    params = {
        "lamin": 24.396308,  # min latitude (Florida Keys)
        "lomin": -125.0,     # min longitude (California)
        "lamax": 49.384358,  # max latitude (Northern border)
        "lomax": -66.93457,  # max longitude (Maine)
    }

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Extract timestamp and state vectors
    timestamp = data.get("time", int(time.time()))
    states = data.get("states", [])

    # Map each flight array into a dict using OpenSky's index reference
    columns = [
        "icao24", "callsign", "origin_country", "time_position", "last_contact",
        "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
        "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
        "spi", "position_source", "category"
    ]

    flights = []
    for s in states:
        record = dict(zip(columns, s[:len(columns)]))
        record["timestamp"] = timestamp
        flights.append(record)

    context["ti"].xcom_push(key="flights_data", value=flights)
    print(f"✅ Retrieved {len(flights)} flights over the U.S. from OpenSky.")
    return flights



def _upload_to_gcs(**context):
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    import json
    from datetime import datetime

    ti = context["ti"]
    flights = ti.xcom_pull(task_ids="fetch_flights", key="flights_data")

    filename = f"opensky_us_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    local_path = f"/opt/airflow/logs/{filename}"

    # Write newline-delimited JSON
    with open(local_path, "w") as f:
        for record in flights:
            json.dump(record, f)
            f.write("\n")

    gcs = GCSHook(gcp_conn_id="google_cloud_default")
    gcs.upload(BUCKET_NAME, filename, local_path)

    print(f"☁️ Uploaded {len(flights)} flights to {BUCKET_NAME}/{filename}")



with DAG(
    "ingest_flight_status_usa",
    default_args=default_args,
    schedule_interval="@hourly",
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
        source_objects=["opensky_us_*.json"],
        destination_project_dataset_table="flight-intel-platform.us_flights_dataset.opensky_flights_raw",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
        write_disposition="WRITE_APPEND",
        gcp_conn_id="google_cloud_default",
    )


    fetch >> upload >> load_bq
