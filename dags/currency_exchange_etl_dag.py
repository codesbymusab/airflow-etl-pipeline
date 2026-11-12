from datetime import datetime, timedelta, timezone
import logging
import os
import requests
from pymongo import MongoClient

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:
    DAG = None
    PythonOperator = None

# Default arguments for the Airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# Configuration
API_URL = "https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY"
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:password123@mongodb:27017/currency_db?authSource=admin",
)
MONGO_DB_NAME = os.getenv("MONGO_DB", "currency_db")
MONGO_COLLECTION_NAME = "exchange_rates"

logger = logging.getLogger(__name__)


def extract_currency_rates(**context):

    ti = context["ti"]
    logger.info("Starting extraction from Frankfurter API: %s", API_URL)

    try:
        response = requests.get(API_URL, timeout=15)
        response.raise_for_status()
        raw_data = response.json()
    except requests.exceptions.RequestException as exc:
        logger.error("Failed to fetch exchange rates from API: %s", exc)
        raise

    logger.info("Extraction successful. Retrieved raw data: %s", raw_data)

    # Named XCom push
    xcom_key = "raw_exchange_rates"
    ti.xcom_push(key=xcom_key, value=raw_data)
    logger.info("Pushed raw exchange rates to Named XCom with key '%s'", xcom_key)
    return f"Extracted {len(raw_data) if isinstance(raw_data, list) else 1} items"


def transform_currency_rates(**context):
  
    ti = context["ti"]
    xcom_pull_key = "raw_exchange_rates"
    raw_data = ti.xcom_pull(key=xcom_pull_key, task_ids="extract_currency_rates")

    if not raw_data:
        raise ValueError(
            f"No data received from upstream task via Named XCom '{xcom_pull_key}'"
        )

    logger.info("Pulled raw data from Named XCom: %s", raw_data)

    retrieved_at_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    transformed_records = []

    # Handle List of quote items (v2 API format):
    # [{"date": "2026-11-12", "base": "EUR", "quote": "USD", "rate": 1.1122}, ...]
    if isinstance(raw_data, list):
        for item in raw_data:
            transformed_record = {
                "base_currency": item.get("base", "EUR"),
                "target_currency": item.get("quote"),
                "exchange_rate": float(item.get("rate")),
                "rate_date": item.get("date"),
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at_timestamp,
            }
            transformed_records.append(transformed_record)

    # Handle dictionary format (v1 API format or wrapper):
    # {"base": "EUR", "date": "2026-11-12", "rates": {"USD": 1.1122, "GBP": 0.835}}
    elif isinstance(raw_data, dict):
        base_cur = raw_data.get("base", "EUR")
        rate_date = raw_data.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        rates_dict = raw_data.get("rates", {})
        for target_cur, rate in rates_dict.items():
            transformed_record = {
                "base_currency": base_cur,
                "target_currency": target_cur,
                "exchange_rate": float(rate),
                "rate_date": rate_date,
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at_timestamp,
            }
            transformed_records.append(transformed_record)
    else:
        raise TypeError(f"Unexpected data format received from API: {type(raw_data)}")

    logger.info("Transformed %d exchange rate records:", len(transformed_records))
    for record in transformed_records:
        logger.info(" -> %s", record)

    # Named XCom push
    xcom_push_key = "transformed_exchange_rates"
    ti.xcom_push(key=xcom_push_key, value=transformed_records)
    logger.info(
        "Pushed %d transformed records to Named XCom with key '%s'",
        len(transformed_records),
        xcom_push_key,
    )
    return transformed_records


def load_currency_rates_to_mongodb(**context):
 
    ti = context["ti"]
    xcom_pull_key = "transformed_exchange_rates"
    transformed_records = ti.xcom_pull(
        key=xcom_pull_key, task_ids="transform_currency_rates"
    )

    if not transformed_records:
        raise ValueError(
            f"No data received from upstream task via Named XCom '{xcom_pull_key}'"
        )

    logger.info(
        "Pulled %d records from Named XCom for MongoDB loading",
        len(transformed_records),
    )

    # Establish MongoDB connection with fallback for local development outside docker
    client = None
    try:
        # First attempt configured URI
        logger.info("Connecting to MongoDB at: %s", MONGO_URI)
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Test connection
        client.admin.command("ping")
    except Exception as exc:
        logger.warning(
            "Primary connection attempt failed: %s. Trying fallback localhost:27017...",
            exc,
        )
        fallback_uri = "mongodb://admin:password123@localhost:27017/currency_db?authSource=admin"
        client = MongoClient(fallback_uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")

    logger.info("Connected to MongoDB successfully.")
    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]

    upserted_count = 0
    inserted_count = 0

    for record in transformed_records:
        # Unique filter on base_currency, target_currency, and rate_date
        filter_query = {
            "base_currency": record["base_currency"],
            "target_currency": record["target_currency"],
            "rate_date": record["rate_date"],
        }
        update_doc = {"$set": record}
        result = collection.update_one(filter_query, update_doc, upsert=True)
        if result.upserted_id is not None:
            inserted_count += 1
        else:
            upserted_count += 1

    total_docs = collection.count_documents({})
    logger.info(
        "MongoDB Load Complete. Newly Inserted: %d, Updated: %d, Total in collection: %d",
        inserted_count,
        upserted_count,
        total_docs,
    )

    summary = {
        "database": MONGO_DB_NAME,
        "collection": MONGO_COLLECTION_NAME,
        "newly_inserted": inserted_count,
        "updated": upserted_count,
        "total_documents": total_docs,
        "status": "SUCCESS",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Named XCom push
    ti.xcom_push(key="load_summary", value=summary)
    logger.info("Pushed load summary to Named XCom with key 'load_summary': %s", summary)

    client.close()
    return summary


# Define the DAG only if Airflow is installed
if DAG is not None and PythonOperator is not None:
    with DAG(
        dag_id="currency_exchange_etl_pipeline",
        default_args=default_args,
        description="ETL Pipeline: Frankfurter Currency Exchange Rates to MongoDB with Named XCom",
        schedule="@daily",
        start_date=datetime(2026, 11, 1),
        catchup=False,
        tags=["etl", "currency", "mongodb", "frankfurter", "astro"],
    ) as dag:

        # Task 1: Extract Currency Exchange Rates
        extract_task = PythonOperator(
            task_id="extract_currency_rates",
            python_callable=extract_currency_rates,
        )

        # Task 2: Transform Exchange Rate Data
        transform_task = PythonOperator(
            task_id="transform_currency_rates",
            python_callable=transform_currency_rates,
        )

        # Task 3: Load Data into MongoDB
        load_task = PythonOperator(
            task_id="load_currency_rates_to_mongodb",
            python_callable=load_currency_rates_to_mongodb,
        )

        # Define DAG task execution dependencies
        extract_task >> transform_task >> load_task
