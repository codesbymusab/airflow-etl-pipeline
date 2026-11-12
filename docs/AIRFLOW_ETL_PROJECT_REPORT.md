# 📊 Engineering Project Report: Airflow Currency Exchange Rates ETL Pipeline

**Project Name:** Astro Airflow MongoDB Currency Exchange ETL Pipeline  
**Author:** Musab (`codesbymusab@gmail.com`)  
**Technology Stack:** Astronomer CLI (Astro CLI), Apache Airflow 3, Docker & Docker Compose, MongoDB 6.0, Mongo Express, Python 3, Frankfurter Public Currency API  
**Repository:** [https://github.com/codesbymusab/airflow-mongodb-currency-etl](https://github.com/codesbymusab/airflow-mongodb-currency-etl)  

---

## 1. Executive Summary

This report documents the design, architecture, implementation, and verification of an automated Data Engineering ETL pipeline developed using the **Astronomer CLI (Astro CLI)** and **Apache Airflow**. The pipeline automates the daily extraction of currency exchange rates from the public **Frankfurter API** (`base=EUR`, `quotes=USD,GBP,JPY`), applies schema transformation, and loads the data into a **MongoDB** NoSQL database with **Mongo Express** as its web-based management interface.

All inter-task data transfers are coordinated via **Airflow Named XComs**. Persistent storage and containerized networking have been implemented using Docker Compose overrides.

---

## 2. System Architecture & Component Diagram

```
 +-------------------------------------------------------------------------------+
 |                            FRANKFURTER PUBLIC API                             |
 |           Endpoint: https://api.frankfurter.dev/v2/rates?base=EUR...          |
 +---------------------------------------+---------------------------------------+
                                         | HTTP GET
                                         v
 +-------------------------------------------------------------------------------+
 |                       ASTRO AIRFLOW ETL CONTAINER POD                         |
 |                                                                               |
 |  [Task 1: extract_currency_rates]                                             |
 |   - Fetches rates for EUR -> USD, GBP, JPY                                    |
 |   - Validates HTTP response                                                   |
 |   - Pushes raw JSON to Named XCom: 'raw_exchange_rates'                       |
 |                                                                               |
 |                                   |                                           |
 |                                   v Named XCom ('raw_exchange_rates')         |
 |                                                                               |
 |  [Task 2: transform_currency_rates]                                           |
 |   - Pulls raw rates from Named XCom                                           |
 |   - Normalizes schema to exact specifications                                 |
 |   - Enriches with 'source' and 'retrieved_at' ISO timestamps                  |
 |   - Pushes transformed list to Named XCom: 'transformed_exchange_rates'       |
 |                                                                               |
 |                                   |                                           |
 |                                   v Named XCom ('transformed_exchange_rates') |
 |                                                                               |
 |  [Task 3: load_currency_rates_to_mongodb]                                     |
 |   - Pulls normalized records from Named XCom                                  |
 |   - Connects to MongoDB via pymongo at mongodb:27017                          |
 |   - Idempotent upsert into 'currency_db.exchange_rates'                       |
 |   - Pushes load summary to Named XCom: 'load_summary'                         |
 +-----------------------------------+-------------------------------------------+
                                     |
                                     v Docker Network (airflow_etl_network)
 +-----------------------------------+-------------------------------------------+
 |                    DOCKER SERVICES & PERSISTENCE LAYER                        |
 |                                                                               |
 |   +------------------------------------+   +-------------------------------+  |
 |   |          MongoDB (mongo:6.0)       |   |   Mongo Express (Web Admin)   |  |
 |   |  Port: 27017                       |   |   Port: 8081                  |  |
 |   |  DB: currency_db                   |   |   URL: http://localhost:8081  |  |
 |   |  Collection: exchange_rates        |   |   Real-time GUI inspector     |  |
 |   +-----------------+------------------+   +---------------+---------------+  |
 |                     |                                      |                  |
 |                     v                                      v                  |
 |       [Persistent Docker Volume]              [Web Administration]            |
 |       mongodb_persistent_storage                 Browser View                 |
 +-------------------------------------------------------------------------------+
```

---

## 3. Infrastructure & Docker Configuration

### 3.1 Docker Networking & Persistent Storage
In accordance with the assignment specifications, MongoDB and Mongo Express were configured in `docker-compose.override.yml`:

- **Docker Networking:**
  - A custom bridge network `airflow_etl_network` was defined.
  - In addition, services were attached to the Astro Airflow internal network `airflow`, granting seamless inter-container name resolution (`mongodb:27017`).
- **Persistent Storage:**
  - A named Docker volume `mongodb_persistent_storage` was mapped to MongoDB's internal directory `/data/db`.
  - Ensures data retention across container lifecycle restarts.
- **Mongo Express Configuration:**
  - Configured as an independent container service exposing web UI on port `8081`.
  - Connected to MongoDB using environment variables: `ME_CONFIG_MONGODB_URL`.

### 3.2 `docker-compose.override.yml` Source Code
```yaml
services:
  mongodb:
    image: mongo:6.0
    container_name: airflow_mongodb
    restart: always
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: password123
      MONGO_INITDB_DATABASE: currency_db
    ports:
      - "27017:27017"
    volumes:
      - mongodb_persistent_storage:/data/db
    networks:
      airflow:
        aliases:
          - mongodb
          - mongo
      airflow_etl_network:
        aliases:
          - mongodb
          - mongo

  mongo-express:
    image: mongo-express:latest
    container_name: airflow_mongo_express
    restart: always
    ports:
      - "8081:8081"
    environment:
      ME_CONFIG_MONGODB_URL: "mongodb://admin:password123@mongodb:27017/?authSource=admin"
      ME_CONFIG_MONGODB_ENABLE_ADMIN: "true"
      ME_CONFIG_MONGODB_AUTH_DATABASE: admin
      ME_CONFIG_BASICAUTH: "false"
    depends_on:
      - mongodb
    networks:
      - airflow
      - airflow_etl_network

volumes:
  mongodb_persistent_storage:
    name: mongodb_persistent_storage
    driver: local

networks:
  airflow:
  airflow_etl_network:
    name: airflow_etl_network
    driver: bridge
```

---

## 4. Airflow DAG Implementation & Named XCom Design

The DAG `currency_exchange_etl_pipeline` is defined in `dags/currency_exchange_etl_dag.py`.

### 4.1 Task 1: `extract_currency_rates`
- **Objective:** Fetch live exchange rates from the Frankfurter API.
- **Endpoint:** `https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY`
- **Named XCom:**
  ```python
  ti.xcom_push(key="raw_exchange_rates", value=raw_data)
  ```

### 4.2 Task 2: `transform_currency_rates`
- **Objective:** Retrieve raw rates, validate structure, format each currency record to match the assignment schema, and enrich with metadata.
- **Named XCom Pull:**
  ```python
  raw_data = ti.xcom_pull(key="raw_exchange_rates", task_ids="extract_currency_rates")
  ```
- **Standardized Schema:**
  ```json
  {
    "base_currency": "EUR",
    "target_currency": "USD",
    "exchange_rate": 1.1117,
    "rate_date": "2026-11-12",
    "source": "Frankfurter API",
    "retrieved_at": "2026-11-12T18:07:22"
  }
  ```
- **Named XCom Push:**
  ```python
  ti.xcom_push(key="transformed_exchange_rates", value=transformed_records)
  ```

### 4.3 Task 3: `load_currency_rates_to_mongodb`
- **Objective:** Connect to MongoDB, ensure collection exists, and upsert each document based on composite key `(base_currency, target_currency, rate_date)`.
- **Named XCom Pull:**
  ```python
  transformed_records = ti.xcom_pull(key="transformed_exchange_rates", task_ids="transform_currency_rates")
  ```
- **Upsert Logic:**
  ```python
  filter_query = {
      "base_currency": record["base_currency"],
      "target_currency": record["target_currency"],
      "rate_date": record["rate_date"]
  }
  collection.update_one(filter_query, {"$set": record}, upsert=True)
  ```
- **Named XCom Push:**
  ```python
  ti.xcom_push(key="load_summary", value=summary)
  ```

---

## 5. Verification & Test Results

### 5.1 Astro Test Suite (`astro dev pytest`)
```
Running your test suite…
Astro Runtime Version: 3.3-12
============================= test session starts ==============================
collected 5 items

tests/dags/test_dag_example.py ...                                       [ 60%]
tests/test_etl_logic.py ..                                               [100%]

======================== 5 passed, 2 warnings in 1.84s =========================
✔ All tests passed!
```

### 5.2 DAG Parsing Validation (`astro dev parse`)
```
Checking your Dags for errors…
Astro Runtime Version: 3.3-12
collected 1 item
.astro/test_dag_integrity_default.py .                                   [100%]
======================== 1 passed in 3.02s =========================
✔ No errors detected in your Dags
```

### 5.3 Live Airflow Execution Log (`astro dev run dags test ...`)
```
[info] [DAG TEST] start task_id=extract_currency_rates
[info] Starting extraction from Frankfurter API: https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY
[info] Extraction successful. Retrieved raw data: [{'date': '2026-11-12', 'base': 'EUR', 'quote': 'GBP', 'rate': 0.83522}, {'date': '2026-11-12', 'base': 'EUR', 'quote': 'JPY', 'rate': 177.25}, {'date': '2026-11-12', 'base': 'EUR', 'quote': 'USD', 'rate': 1.1117}]
[info] Pushed raw exchange rates to Named XCom with key 'raw_exchange_rates'
[info] Task instance state updated: new_state=success

[info] [DAG TEST] start task_id=transform_currency_rates
[info] Pulled raw data from Named XCom: [{'date': '2026-11-12', 'base': 'EUR', 'quote': 'GBP', 'rate': 0.83522}, ...]
[info] Transformed 3 exchange rate records:
       -> {'base_currency': 'EUR', 'target_currency': 'GBP', 'exchange_rate': 0.83522, 'rate_date': '2026-11-12', 'source': 'Frankfurter API', 'retrieved_at': '2026-11-12T18:07:22'}
       -> {'base_currency': 'EUR', 'target_currency': 'JPY', 'exchange_rate': 177.25, 'rate_date': '2026-11-12', 'source': 'Frankfurter API', 'retrieved_at': '2026-11-12T18:07:22'}
       -> {'base_currency': 'EUR', 'target_currency': 'USD', 'exchange_rate': 1.1117, 'rate_date': '2026-11-12', 'source': 'Frankfurter API', 'retrieved_at': '2026-11-12T18:07:22'}
[info] Pushed 3 transformed records to Named XCom with key 'transformed_exchange_rates'
[info] Task instance state updated: new_state=success

[info] [DAG TEST] start task_id=load_currency_rates_to_mongodb
[info] Pulled 3 records from Named XCom for MongoDB loading
[info] Connecting to MongoDB at: mongodb://admin:password123@mongodb:27017/currency_db?authSource=admin
[info] Connected to MongoDB successfully.
[info] MongoDB Load Complete. Newly Inserted: 0, Updated: 3, Total in collection: 3
[info] Pushed load summary to Named XCom with key 'load_summary': {'database': 'currency_db', 'collection': 'exchange_rates', 'newly_inserted': 0, 'updated': 3, 'total_documents': 3, 'status': 'SUCCESS', 'timestamp': '2026-11-12T18:07:24.450575+00:00'}
[info] Task instance state updated: new_state=success

[info] DagRun Finished: dag_id=currency_exchange_etl_pipeline, state=success, run_duration=5.246616
```

### 5.4 MongoDB Direct Verification
Querying `currency_db.exchange_rates` from Python:
```python
>>> client.currency_db.exchange_rates.find({}, {"_id": 0})
[
  {
    "base_currency": "EUR",
    "target_currency": "USD",
    "exchange_rate": 1.1117,
    "rate_date": "2026-11-12",
    "source": "Frankfurter API",
    "retrieved_at": "2026-11-12T18:07:22"
  },
  {
    "base_currency": "EUR",
    "target_currency": "GBP",
    "exchange_rate": 0.83522,
    "rate_date": "2026-11-12",
    "source": "Frankfurter API",
    "retrieved_at": "2026-11-12T18:07:22"
  },
  {
    "base_currency": "EUR",
    "target_currency": "JPY",
    "exchange_rate": 177.25,
    "rate_date": "2026-11-12",
    "source": "Frankfurter API",
    "retrieved_at": "2026-11-12T18:07:22"
  }
]
```

---

## 6. Screenshots Gallery

### Screenshot 1: Airflow DAG Overview & Task Graph
*Shows the DAG `currency_exchange_etl_pipeline` with the sequential dependencies of the 3 tasks.*
![Airflow DAG Details](screenshots/07_airflow_dag_details.png)

### Screenshot 2: Airflow Task Execution Grid
*Shows the execution status and metrics for `extract_currency_rates`, `transform_currency_rates`, and `load_currency_rates_to_mongodb`.*
![Airflow DAG Tasks Grid](screenshots/08_airflow_dag_tasks.png)

### Screenshot 3: Airflow DAG Source Code in Web UI
*Shows the PythonOperator callable definitions and Named XCom push/pull logic within Airflow.*
![Airflow DAG Code](screenshots/09_airflow_dag_code.png)

### Screenshot 4: Mongo Express - Database Overview
*Shows the active databases in MongoDB, including `currency_db` and collection `exchange_rates`.*
![Mongo Express Database](screenshots/02_mongo_express_currency_db.png)

### Screenshot 5: Mongo Express - Transformed Documents Table
*Shows the live currency exchange rate records stored in MongoDB.*
![Mongo Express Documents Table](screenshots/03_mongo_express_documents_table.png)

### Screenshot 6: Mongo Express - Document Detail Modal
*Shows the exact JSON structure of a single transformed currency record in MongoDB.*
![Mongo Express Document Detail](screenshots/04_mongo_express_document_detail.png)

### Screenshot 7: Astro Airflow Web UI Dashboard
*Shows the Airflow system health, active scheduler, and triggered DAG runs.*
![Airflow Web UI Dashboard](screenshots/05_airflow_web_ui.png)

---

## 7. Submission Instructions & Repository Link

### 7.1 GitHub / DagsHub Repository
The complete project codebase, Docker configurations, and tests are prepared for pushing to GitHub / DagsHub:

**Repository URL:** [https://github.com/codesbymusab/airflow-mongodb-currency-etl](https://github.com/codesbymusab/airflow-mongodb-currency-etl)

To push the repository to your remote:
```bash
# Add your GitHub or DagsHub remote repository:
git remote add origin https://github.com/codesbymusab/airflow-mongodb-currency-etl.git

# Push to main branch:
git branch -M main
git push -u origin main
```

---

## 8. Conclusion
The Astro Airflow ETL pipeline meets every assignment requirement:
- Initialized and deployed with Astro CLI.
- Integrated with Frankfurter public exchange rates API.
- Configured MongoDB and Mongo Express with Docker networking and persistent volume storage.
- Three dedicated tasks for Extract, Transform, and Load.
- Named XComs utilized for cross-task communication.
- Validated via automated pytest suite, DAG parse checks, and end-to-end Airflow execution.
