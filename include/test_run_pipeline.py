"""
Direct execution test runner for currency_exchange_etl_pipeline tasks
Simulates Airflow TaskInstance context and Named XCom push/pull
"""

import sys
import os

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dags.currency_exchange_etl_dag import (
    extract_currency_rates,
    transform_currency_rates,
    load_currency_rates_to_mongodb,
)


class MockTaskInstance:
    """Mock Airflow TaskInstance supporting Named XCom push & pull"""

    def __init__(self):
        self.xcom_store = {}

    def xcom_push(self, key, value):
        self.xcom_store[key] = value
        print(f"[Airflow XCom Push] Key='{key}', Type={type(value).__name__}")

    def xcom_pull(self, key, task_ids=None):
        val = self.xcom_store.get(key)
        print(f"[Airflow XCom Pull] Key='{key}' from Task='{task_ids}' -> Found: {val is not None}")
        return val


def main():
    ti = MockTaskInstance()
    context = {"ti": ti}

    print("==================================================")
    print("STEP 1: Executing Task 1 (extract_currency_rates)")
    print("==================================================")
    extract_currency_rates(**context)

    print("\n==================================================")
    print("STEP 2: Executing Task 2 (transform_currency_rates)")
    print("==================================================")
    transform_currency_rates(**context)

    print("\n==================================================")
    print("STEP 3: Executing Task 3 (load_currency_rates_to_mongodb)")
    print("==================================================")
    # Ensure MONGO_URI connects to localhost:27017 for host execution
    os.environ["MONGO_URI"] = "mongodb://admin:password123@localhost:27017/currency_db?authSource=admin"
    summary = load_currency_rates_to_mongodb(**context)

    print("\n==================================================")
    print("ETL PIPELINE EXECUTION SUCCESSFUL!")
    print(f"Summary: {summary}")
    print("==================================================")


if __name__ == "__main__":
    main()
