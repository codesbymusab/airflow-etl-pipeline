# 🚀 Airflow Currency Exchange Rates ETL Pipeline (Astro CLI + MongoDB + Mongo Express)

An end-to-end automated Data Engineering ETL pipeline developed using **Astronomer CLI (Astro CLI)** and **Apache Airflow**. This pipeline extracts daily currency exchange rates from the public **Frankfurter API**, performs schema transformation with metadata enrichment, and loads the transformed records into **MongoDB** backed by persistent Docker volume storage and monitored in real time via **Mongo Express**.

---

## 📌 Architecture Overview

```
                          ┌──────────────────────────┐
                          │   Frankfurter API (v2)   │
                          │   api.frankfurter.dev    │
                          └─────────────┬────────────┘
                                        │ (HTTP GET)
                                        ▼
                   ┌─────────────────────────────────────────┐
                   │  Task 1: extract_currency_rates         │
                   │  - Pulls rates for EUR -> USD, GBP, JPY │
                   │  - Pushes raw data to Named XCom        │
                   └────────────────────┬────────────────────┘
                                        │ Named XCom: 'raw_exchange_rates'
                                        ▼
                   ┌─────────────────────────────────────────┐
                   │  Task 2: transform_currency_rates       │
                   │  - Normalizes JSON schema               │
                   │  - Appends source and retrieved_at      │
                   │  - Pushes records to Named XCom         │
                   └────────────────────┬────────────────────┘
                                        │ Named XCom: 'transformed_exchange_rates'
                                        ▼
                   ┌─────────────────────────────────────────┐
                   │  Task 3: load_currency_rates_to_mongodb │
                   │  - Connects to MongoDB (port 27017)     │
                   │  - Upserts into 'exchange_rates' col.   │
                   │  - Pushes execution summary to XCom     │
                   └────────────────────┬────────────────────┘
                                        │
                    ┌───────────────────┴───────────────────┐
                    ▼                                       ▼
       ┌─────────────────────────┐             ┌─────────────────────────┐
       │   MongoDB Database      │◄────────────┤   Mongo Express Web UI  │
       │   Port: 27017           │ (Admin/GUI) │   Port: 8081            │
       │   Vol: mongodb_storage  │             │   Browser Dashboard     │
       └─────────────────────────┘             └─────────────────────────┘
```

---

## 🧩 Key Project Features

### 1. Astro CLI Project Structure
- Standard Astro Airflow architecture initialized via `astro dev init`.
- Astro Runtime with Airflow 3 and Python 3.
- Integrated testing via `astro dev pytest` and `astro dev parse`.

### 2. Frankfurter API Integration
- **API URL:** `https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY`
- Retrieves live daily currency exchange rates for Euro (`EUR`) against US Dollar (`USD`), British Pound (`GBP`), and Japanese Yen (`JPY`).

### 3. Docker Architecture & Meta Details
- **Docker Networking:** Both MongoDB and Mongo Express join the custom bridge network (`airflow_etl_network`) and the Astro internal network (`airflow`), allowing inter-container hostname resolution (`mongodb:27017`).
- **Persistent Storage:** MongoDB utilizes a named persistent Docker volume (`mongodb_persistent_storage`) mounted to `/data/db`, ensuring data durability across container restarts.
- **Mongo Express Service:** Configured in `docker-compose.override.yml` as a dedicated web-based administration interface running at `http://localhost:8081`.

### 4. Three (3) Modular Airflow Tasks
1. **`extract_currency_rates`**: Queries Frankfurter API, validates HTTP 200 response, and pushes raw JSON data to Named XCom key `raw_exchange_rates`.
2. **`transform_currency_rates`**: Pulls from `raw_exchange_rates`, standardizes each currency record, appends metadata (`source: "Frankfurter API"`, `retrieved_at: <ISO timestamp>`), and pushes to Named XCom key `transformed_exchange_rates`.
3. **`load_currency_rates_to_mongodb`**: Pulls from `transformed_exchange_rates`, connects to MongoDB via `pymongo`, executes idempotent upserts (`update_one(..., upsert=True)`) to collection `exchange_rates` under database `currency_db`, and pushes final load metrics to Named XCom key `load_summary`.

### 5. Airflow Named XCom Communication
- Strict adherence to explicit Named XCom keys (`key="raw_exchange_rates"`, `key="transformed_exchange_rates"`, `key="load_summary"`).

---

## 📋 Standardized Data Schema

Each document stored in MongoDB matches the required schema:

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

---

## 🚀 Quick Start Guide

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (v20.10+)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) (v1.40+)
- Python 3.10+ (for local scripts)

### Installation & Execution

1. **Clone the repository:**
   ```bash
   git clone https://github.com/codesbymusab/airflow-mongodb-currency-etl.git
   cd airflow-mongodb-currency-etl
   ```

2. **Configure environment:**
   ```bash
   cp .env.example .env
   ```

3. **Validate DAG integrity and run test suite:**
   ```bash
   astro dev parse
   astro dev pytest
   ```

4. **Start Airflow and Docker services:**
   ```bash
   astro dev start
   ```

5. **Access Web Interfaces:**
   - **Airflow Web UI:** [http://localhost:8080](http://localhost:8080)
   - **Mongo Express Web UI:** [http://localhost:8081](http://localhost:8081)
   - **MongoDB Direct:** `mongodb://admin:password123@localhost:27017`

6. **Trigger the ETL DAG:**
   ```bash
   astro dev run dags trigger currency_exchange_etl_pipeline
   ```

---

## 🧪 Testing & Verification

Run tests inside the isolated Astro container:
```bash
astro dev pytest
```
Output:
```
tests/dags/test_dag_example.py ...    [ 60%]
tests/test_etl_logic.py ..            [100%]
================ 5 passed in 1.84s =================
```

---

## 📷 Screenshots

| Component | Description | Screenshot |
|---|---|---|
| **Airflow DAG Graph** | 3-task dependency pipeline | `docs/screenshots/07_airflow_dag_details.png` |
| **Airflow Task Status** | Successful execution of all tasks | `docs/screenshots/08_airflow_dag_tasks.png` |
| **Mongo Express DB** | `currency_db` overview | `docs/screenshots/02_mongo_express_currency_db.png` |
| **Mongo Express Records** | Transformed documents table | `docs/screenshots/03_mongo_express_documents_table.png` |
| **Mongo Express JSON** | Detailed document structure | `docs/screenshots/04_mongo_express_document_detail.png` |

---

## 📄 License
This project is open-source and available under the [MIT License](LICENSE).
