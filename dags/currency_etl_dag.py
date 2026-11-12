"""
Daily currency exchange-rate ETL: Frankfurter API -> transform -> MongoDB.

Tasks (data passed between them with named XComs):
  1. extract_exchange_rates    pushes key="raw_rates"
  2. transform_exchange_rates  pulls "raw_rates", pushes key="transformed_rates"
  3. load_exchange_rates       pulls "transformed_rates", upserts into MongoDB
"""
import logging
import os
from datetime import datetime, timezone

import requests
from pymongo import ASCENDING, MongoClient, UpdateOne

from airflow.sdk import dag, task

log = logging.getLogger(__name__)

API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
QUOTES = "USD,GBP,JPY,CAD"
SOURCE = "Frankfurter API"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin123@mongo:27017/?authSource=admin")
MONGO_DB = os.getenv("MONGO_DB", "currency_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "exchange_rates")


@dag(
    dag_id="currency_exchange_etl",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    default_args={"owner": "ali", "retries": 2},
    tags=["etl", "currency", "mongodb"],
)
def currency_exchange_etl():

    @task
    def extract_exchange_rates(**context):
        """Call Frankfurter API and push the raw response as XCom 'raw_rates'."""
        resp = requests.get(
            API_URL, params={"base": BASE_CURRENCY, "quotes": QUOTES}, timeout=30
        )
        resp.raise_for_status()
        raw = resp.json()
        if not raw:
            raise ValueError("Frankfurter API returned no data")
        log.info("Extracted %d rates", len(raw))
        context["ti"].xcom_push(key="raw_rates", value=raw)

    @task
    def transform_exchange_rates(**context):
        """Reshape raw rates into the required document format."""
        ti = context["ti"]
        raw = ti.xcom_pull(task_ids="extract_exchange_rates", key="raw_rates")
        retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

        docs = [
            {
                "base_currency": item["base"],
                "target_currency": item["quote"],
                "exchange_rate": float(item["rate"]),
                "rate_date": item["date"],
                "source": SOURCE,
                "retrieved_at": retrieved_at,
            }
            for item in raw
        ]
        log.info("Transformed %d documents", len(docs))
        ti.xcom_push(key="transformed_rates", value=docs)

    @task
    def load_exchange_rates(**context):
        """Upsert transformed documents into MongoDB (idempotent per day/pair)."""
        docs = context["ti"].xcom_pull(
            task_ids="transform_exchange_rates", key="transformed_rates"
        )
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        try:
            col = client[MONGO_DB][MONGO_COLLECTION]
            col.create_index(
                [
                    ("base_currency", ASCENDING),
                    ("target_currency", ASCENDING),
                    ("rate_date", ASCENDING),
                ],
                unique=True,
            )
            ops = [
                UpdateOne(
                    {
                        "base_currency": d["base_currency"],
                        "target_currency": d["target_currency"],
                        "rate_date": d["rate_date"],
                    },
                    {"$set": d},
                    upsert=True,
                )
                for d in docs
            ]
            result = col.bulk_write(ops)
            log.info(
                "Loaded into %s.%s: upserted=%d modified=%d",
                MONGO_DB, MONGO_COLLECTION, result.upserted_count, result.modified_count,
            )
        finally:
            client.close()

    extract_exchange_rates() >> transform_exchange_rates() >> load_exchange_rates()


currency_exchange_etl()