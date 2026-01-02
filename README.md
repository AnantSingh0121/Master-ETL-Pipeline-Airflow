# Master ETL Pipeline (Airflow)

A robust Data Engineering pipeline built with Apache Airflow that orchestrates three distinct ETL processes: **Market Data**, **Aviation Data** and **Weather Data**. The pipeline extracts data from external REST APIs, transforms it into structured formats and loads it into a PostgreSQL data warehouse with built-in data quality checks.



## Features

* **Multi-Source Integration**: Synchronizes data from Marketstack, Aviationstack and Open-Meteo.
* **Taskflow API**: Utilizes Airflow's modern `@task` decorators for clean, Pythonic DAG definitions.
* **Idempotency**: Implements `ON CONFLICT` and duplicate removal logic to ensure the pipeline can be safely re-run.
* **Data Quality Assurance**: Includes dedicated validation tasks to verify records exist in the database after the load phase.
* **Secure Credential Management**: Uses Airflow Connections and Environment Variables for API keys.

---

## Architecture

The DAG follows a classic **Extract-Transform-Load-Quality (ETLQ)** pattern for each data domain:

1.  **Extract**: Fetches raw JSON data using `HttpHook`.
2.  **Transform**: Cleanses and structures data (e.g., parsing dates, casting types).
3.  **Load**: Ensures target tables exist and upserts data into PostgreSQL using `PostgresHook`.
4.  **Quality Check**: Validates that the data was actually written to the destination.

---

## Setup & Configuration

### 1. Airflow Connections
You must configure the following connections in the Airflow UI (**Admin -> Connections**):

| Conn ID | Conn Type | Host |
| :--- | :--- | :--- |
| `marketstack_api` | HTTP | `https://api.marketstack.com` |
| `aviationstack_api` | HTTP | `https://api.aviationstack.com` |
| `open_meteo_api` | HTTP | `https://api.open-meteo.com` |
| `bbc_news_api` | HTTP | `https://BBC-News-API.proxy-production.allthingsdev.co` |
| `postgres_default` | Postgres | *Your Database Credentials* |

### 2. Environment Variables
To keep API keys secure, set the following variables in your environment or `.env` file:
* `MARKETSTACK_API_KEY`: Your Marketstack access key.
* `AVIATIONSTACK_API_KEY`: Your Aviationstack access key.

---

## Database Schema

The pipeline automatically manages the following tables in PostgreSQL:

* **`stock_prices`**: Tracks symbol, close price, volume and trade date (Unique: symbol + date).
* **`aviation_data`**: Tracks flight dates, airline names and airport movements (Unique: flight number + date).
* **`weather_data`**: Tracks temperature and wind conditions for specific coordinates with automated timestamps.

---

## DAG Details

* **Schedule**: `@daily`
* **Catchup**: `False`
* **Retries**: 2 (with a 5-minute delay)
* **Tags**: `master`, `etl`
---

## Usage

1. Copy the DAG script into your Airflow `dags/` folder.
2. Ensure the `apache-airflow-providers-http` and `apache-airflow-providers-postgres` packages are installed.
3. Unpause the `master_etl_pipeline` in the Airflow Web UI.
