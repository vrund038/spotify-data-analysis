import json
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import psycopg2
from dotenv import load_dotenv

# ------------------------------------------------------
# LOAD ENV VARIABLES
# ------------------------------------------------------
load_dotenv(dotenv_path="/opt/airflow/dags/.env")

# ------------------------------------------------------
# MINIO CONFIG
# ------------------------------------------------------

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_PREFIX = os.getenv("MINIO_PREFIX")

# ------------------------------------------------------
# POSTGRES CONFIG
# ------------------------------------------------------

PG_HOST = os.getenv("PG_HOST")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT", 5432)

LOCAL_TEMP_PATH = os.getenv("LOCAL_TEMP_PATH", "/tmp/spotify_raw.json")


# ------------------------------------------------------
# EXTRACT
# ------------------------------------------------------

def extract_from_minio():

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    response = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=MINIO_PREFIX)
    contents = response.get("Contents", [])

    all_events = []

    for obj in contents:
        key = obj["Key"]

        if not key.endswith(".json"):
            continue

        data = s3.get_object(Bucket=MINIO_BUCKET, Key=key)
        lines = data["Body"].read().decode("utf-8").splitlines()

        for line in lines:
            try:
                all_events.append(json.loads(line))
            except:
                continue

    with open(LOCAL_TEMP_PATH, "w") as f:
        json.dump(all_events, f)

    print(f"Extracted {len(all_events)} events")

    return LOCAL_TEMP_PATH


# ------------------------------------------------------
# LOAD TO POSTGRES (BRONZE)
# ------------------------------------------------------

def load_raw_to_postgres(**context):

    file_path = context["ti"].xcom_pull(task_ids="extract_data")

    with open(file_path, "r") as f:
        events = json.load(f)

    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT
    )

    cur = conn.cursor()

    # Create Schemas
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS bronze;
    """)

    # Create Bronze Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bronze.spotify_events (
            event_id TEXT,
            user_id TEXT,
            song_id TEXT,
            artist_name TEXT,
            song_name TEXT,
            event_type TEXT,
            device_type TEXT,
            country TEXT,
            timestamp TIMESTAMP
        );
    """)

    insert_sql = """
        INSERT INTO bronze.spotify_events (
            event_id,
            user_id,
            song_id,
            artist_name,
            song_name,
            event_type,
            device_type,
            country,
            timestamp
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    for event in events:
        cur.execute(insert_sql, (
            event.get("event_id"),
            event.get("user_id"),
            event.get("song_id"),
            event.get("artist_name"),
            event.get("song_name"),
            event.get("event_type"),
            event.get("device_type"),
            event.get("country"),
            event.get("timestamp")
        ))

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(events)} records into bronze.spotify_events")


# ------------------------------------------------------
# DAG
# ------------------------------------------------------

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 10, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "spotify_minio_to_postgres_bronze",
    default_args=default_args,
    description="Load Spotify data into Postgres Bronze",
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_from_minio
    )

    load_task = PythonOperator(
        task_id="load_raw_to_postgres",
        python_callable=load_raw_to_postgres
    )

    extract_task >> load_task