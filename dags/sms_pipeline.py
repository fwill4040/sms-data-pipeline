from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import sqlite3
import logging
import os

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "spam.csv")
DB_PATH = os.path.join(BASE_DIR, "data", "pipeline.db")
API_URL = os.getenv("SPAM_API_URL", "http://host.docker.internal:8000/predict")

default_args = {
    "owner": "fikree",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def ingest(**context):
    logger.info(f"Loading data from {DATA_PATH}")
    df = pd.read_csv(DATA_PATH, encoding="latin-1")
    df = df[["v1", "v2"]].rename(columns={"v1": "label", "v2": "text"})
    df = df.head(50)
    context["ti"].xcom_push(key="raw_data", value=df.to_json())
    logger.info(f"Ingested {len(df)} rows")
    return len(df)


def transform(**context):
    raw_json = context["ti"].xcom_pull(key="raw_data", task_ids="ingest")
    df = pd.read_json(raw_json)
    before = len(df)
    df = df.dropna()
    df["text"] = df["text"].str.strip()
    df = df[df["text"].str.len() > 0]
    df = df.drop_duplicates(subset=["text"])
    after = len(df)
    logger.info(f"Transformed {before} -> {after} rows")
    context["ti"].xcom_push(key="clean_data", value=df.to_json())
    return after


def classify(**context):
    clean_json = context["ti"].xcom_pull(key="clean_data", task_ids="transform")
    df = pd.read_json(clean_json)
    results = []
    errors = 0
    for _, row in df.iterrows():
        try:
            response = requests.post(
                API_URL,
                json={"text": row["text"]},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                results.append({
                    "text": row["text"],
                    "true_label": row["label"],
                    "predicted_label": data["label"],
                    "confidence": data["confidence"],
                    "is_spam": data["is_spam"]
                })
            else:
                errors += 1
        except Exception as e:
            logger.error(f"Error classifying: {e}")
            errors += 1

    logger.info(f"Classified {len(results)} rows, {errors} errors")
    results_df = pd.DataFrame(results)
    context["ti"].xcom_push(key="results", value=results_df.to_json())
    context["ti"].xcom_push(key="errors", value=errors)
    return len(results)


def store(**context):
    results_json = context["ti"].xcom_pull(key="results", task_ids="classify")
    errors = context["ti"].xcom_pull(key="errors", task_ids="classify")
    df = pd.read_json(results_json)

    conn = sqlite3.connect(DB_PATH)
    df["processed_at"] = datetime.now().isoformat()
    df.to_sql("sms_classifications", conn, if_exists="append", index=False)

    total = len(df)
    spam_count = df["is_spam"].sum()
    accuracy = (df["true_label"] == df["predicted_label"]).mean() * 100

    summary = pd.DataFrame([{
        "run_at": datetime.now().isoformat(),
        "total_processed": total,
        "spam_detected": int(spam_count),
        "ham_detected": int(total - spam_count),
        "accuracy_pct": round(accuracy, 2),
        "errors": errors
    }])
    summary.to_sql("pipeline_runs", conn, if_exists="append", index=False)
    conn.close()

    logger.info(f"Stored {total} results. Spam: {spam_count}, Accuracy: {accuracy:.1f}%")
    return total


with DAG(
    dag_id="sms_classification_pipeline",
    default_args=default_args,
    description="Ingest, transform, classify and store SMS messages",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mlops", "spam", "portfolio"]
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    classify_task = PythonOperator(
        task_id="classify",
        python_callable=classify
    )

    store_task = PythonOperator(
        task_id="store",
        python_callable=store
    )

    ingest_task >> transform_task >> classify_task >> store_task