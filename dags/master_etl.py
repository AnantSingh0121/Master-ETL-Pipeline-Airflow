from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta
import os

default_args={
    "owner":"airflow",
    "retries":2,
    "retry_delay":timedelta(minutes=5),
    "start_date":datetime(2025,1,1),
}

with DAG(
    dag_id="master_etl_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["master","etl"]
) as dag:

    # -------------------- MARKET ETL --------------------
    @task
    def extract_market():
        http=HttpHook(method="GET",http_conn_id="marketstack_api")
        api_key=os.getenv("MARKETSTACK_API_KEY")
        response=http.run(
            endpoint="/v1/eod",
            data={"access_key":api_key,"symbols":"AAPL","limit":1}
        )
        return response.json()["data"][0]

    @task
    def transform_market(raw):
        return {
            "symbol":raw["symbol"],
            "close_price":float(raw["close"]),
            "volume":int(raw["volume"]),
            "trade_date":raw["date"][:10]
        }

    @task
    def load_market(clean):
        pg=PostgresHook(postgres_conn_id="postgres_default")
        pg.run("""
            CREATE TABLE IF NOT EXISTS stock_prices(
                symbol TEXT,
                close_price FLOAT,
                volume BIGINT,
                trade_date DATE
            );
        """)
        pg.run("""
            DELETE FROM stock_prices
            WHERE ctid NOT IN (
                SELECT min(ctid)
                FROM stock_prices
                GROUP BY symbol, trade_date
            );
        """)
        pg.run("""
            DO $$
            BEGIN
                IF NOT EXISTS(
                    SELECT 1 FROM pg_constraint WHERE conname='stock_unique'
                ) THEN
                    ALTER TABLE stock_prices ADD CONSTRAINT stock_unique UNIQUE(symbol,trade_date);
                END IF;
            END$$;
        """)
        pg.run("""
            INSERT INTO stock_prices(symbol,close_price,volume,trade_date)
            VALUES(%s,%s,%s,%s)
            ON CONFLICT(symbol,trade_date) DO NOTHING;
        """,parameters=(clean["symbol"],clean["close_price"],clean["volume"],clean["trade_date"]))
        return clean["trade_date"]

    @task
    def quality_market(trade_date):
        pg=PostgresHook(postgres_conn_id="postgres_default")
        count=pg.get_first("SELECT COUNT(*) FROM stock_prices WHERE trade_date=%s;", parameters=(trade_date,))[0]
        if count==0:
            raise ValueError(f"Market ETL failed data quality check for date {trade_date}")
        return True

    # -------------------- AVIATION ETL --------------------
    @task
    def extract_aviation():
        http=HttpHook(method="GET",http_conn_id="aviationstack_api")
        api_key=os.getenv("AVIATIONSTACK_API_KEY")
        response=http.run(endpoint=f"/v1/flights?access_key={api_key}&limit=1")
        return response.json()["data"][0]

    @task
    def transform_aviation(flight):
        return {
            "flight_date":flight["flight_date"],
            "flight_number":flight["flight"]["number"],
            "airline":flight["airline"]["name"],
            "departure":flight["departure"]["airport"],
            "arrival":flight["arrival"]["airport"]
        }

    @task
    def load_aviation(row):
        pg=PostgresHook(postgres_conn_id="postgres_default")
        pg.run("""
            CREATE TABLE IF NOT EXISTS aviation_data(
                flight_date DATE,
                flight_number TEXT,
                airline TEXT,
                departure TEXT,
                arrival TEXT
            );
        """)
        pg.run("""
            DELETE FROM aviation_data
            WHERE ctid NOT IN (
                SELECT min(ctid)
                FROM aviation_data
                GROUP BY flight_number, flight_date
            );
        """)
        pg.run("""
            DO $$
            BEGIN
                IF NOT EXISTS(
                    SELECT 1 FROM pg_constraint WHERE conname='flight_unique'
                ) THEN
                    ALTER TABLE aviation_data ADD CONSTRAINT flight_unique UNIQUE(flight_number,flight_date);
                END IF;
            END$$;
        """)
        pg.run("""
            INSERT INTO aviation_data(flight_date,flight_number,airline,departure,arrival)
            VALUES(%s,%s,%s,%s,%s)
            ON CONFLICT(flight_number,flight_date) DO NOTHING;
        """,parameters=(row["flight_date"],row["flight_number"],row["airline"],row["departure"],row["arrival"]))
        return row["flight_date"]

    @task
    def quality_aviation(flight_date):
        pg=PostgresHook(postgres_conn_id="postgres_default")
        count=pg.get_first("SELECT COUNT(*) FROM aviation_data WHERE flight_date=%s;", parameters=(flight_date,))[0]
        if count==0:
            raise ValueError(f"Aviation ETL failed data quality check for date {flight_date}")
        return True
    LATITUDE="51.5074"
    LONGITUDE="-0.1278"

    @task
    def extract_weather():
        http=HttpHook(method="GET",http_conn_id="open_meteo_api")
        endpoint=f"/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"
        response=http.run(endpoint)
        if response.status_code==200:
            return response.json()
        else:
            raise ValueError(f"Weather ETL failed: {response.status_code}")

    @task
    def transform_weather(data):
        current=data["current_weather"]
        return {
            "latitude":LATITUDE,
            "longitude":LONGITUDE,
            "temperature":current["temperature"],
            "windspeed":current["windspeed"],
            "winddirection":current["winddirection"],
            "weathercode":current["weathercode"]
        }

    @task
    def load_weather(clean):
        pg=PostgresHook(postgres_conn_id="postgres_default")
        pg.run("""
            CREATE TABLE IF NOT EXISTS weather_data(
                id SERIAL PRIMARY KEY,
                latitude VARCHAR(50),
                longitude VARCHAR(50),
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        pg.run("""
            INSERT INTO weather_data(latitude,longitude,temperature,windspeed,winddirection,weathercode,recorded_at)
            VALUES(%s,%s,%s,%s,%s,%s,NOW());
        """,parameters=(clean["latitude"],clean["longitude"],clean["temperature"],clean["windspeed"],clean["winddirection"],clean["weathercode"]))
        return True

    @task
    def quality_weather(_):
        pg=PostgresHook(postgres_conn_id="postgres_default")
        count=pg.get_first("SELECT COUNT(*) FROM weather_data WHERE DATE(recorded_at)=CURRENT_DATE;")[0]
        if count==0:
            raise ValueError("Weather ETL failed data quality check")
        return True
    market_raw=extract_market()
    aviation_raw=extract_aviation()
    weather_raw=extract_weather()
    market_clean=transform_market(market_raw)
    aviation_clean=transform_aviation(aviation_raw)
    weather_clean=transform_weather(weather_raw)
    market_date=load_market(market_clean)
    aviation_date=load_aviation(aviation_clean)
    weather_loaded=load_weather(weather_clean)
    quality_market(market_date)
    quality_aviation(aviation_date)
    quality_weather(weather_loaded)
