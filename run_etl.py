"""
run_etl.py
----------
Simple end-to-end ETL pipeline for NYC TLC Yellow Taxi data.
No PySpark, no Java, no Airflow - just Python + Pandas + DuckDB.

Run: python run_etl.py
"""

import logging
import requests
from pathlib import Path
import pandas as pd
import duckdb

# ── Setup logging ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
YEAR       = 2023
MONTH      = 1
RAW_DIR    = Path("data/raw")
OUTPUT_DIR = Path("data/processed")
DB_PATH    = Path("data/warehouse.duckdb")
DATA_URL   = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{YEAR}-{MONTH:02d}.parquet"
RAW_FILE   = RAW_DIR / f"yellow_tripdata_{YEAR}-{MONTH:02d}.parquet"

RAW_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: EXTRACT
# ─────────────────────────────────────────────────────────────────────────────
def extract():
    if RAW_FILE.exists():
        log.info(f"Already downloaded: {RAW_FILE}")
        return

    log.info("Downloading from NYC TLC open data...")
    response = requests.get(DATA_URL, stream=True, timeout=120)
    response.raise_for_status()

    total_mb = int(response.headers.get("content-length", 0)) / 1024 / 1024
    downloaded = 0

    with open(RAW_FILE, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            downloaded += len(chunk) / 1024 / 1024
            log.info(f"   {downloaded:.1f} MB / {total_mb:.1f} MB")

    log.info(f"Saved to: {RAW_FILE}")


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: TRANSFORM
# ─────────────────────────────────────────────────────────────────────────────
def transform() -> pd.DataFrame:
    log.info("Reading parquet file...")
    df = pd.read_parquet(RAW_FILE)
    log.info(f"Raw rows: {len(df):,}")

    # Rename columns
    df = df.rename(columns={
        "tpep_pickup_datetime":  "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID":          "pickup_location_id",
        "DOLocationID":          "dropoff_location_id",
        "VendorID":              "vendor_id",
        "RatecodeID":            "rate_code_id",
    })

    # Drop nulls
    df = df.dropna(subset=["pickup_datetime", "dropoff_datetime", "fare_amount", "trip_distance"])

    # Correct types
    df["pickup_datetime"]  = pd.to_datetime(df["pickup_datetime"])
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"])
    df["fare_amount"]      = pd.to_numeric(df["fare_amount"],      errors="coerce")
    df["trip_distance"]    = pd.to_numeric(df["trip_distance"],    errors="coerce")
    df["tip_amount"]       = pd.to_numeric(df["tip_amount"],       errors="coerce").fillna(0)
    df["total_amount"]     = pd.to_numeric(df["total_amount"],     errors="coerce").fillna(0)
    df["passenger_count"]  = pd.to_numeric(df["passenger_count"],  errors="coerce").fillna(1)

    # Time features
    df["pickup_year"]  = df["pickup_datetime"].dt.year
    df["pickup_month"] = df["pickup_datetime"].dt.month
    df["pickup_day"]   = df["pickup_datetime"].dt.day
    df["pickup_hour"]  = df["pickup_datetime"].dt.hour
    df["pickup_dow"]   = df["pickup_datetime"].dt.dayofweek
    df["is_weekend"]   = df["pickup_dow"].isin([5, 6])
    df["time_of_day"]  = pd.cut(
        df["pickup_hour"],
        bins=[-1, 4, 11, 16, 20, 23],
        labels=["night", "morning", "afternoon", "evening", "night2"]
    ).astype(str).replace("night2", "night")

    # Trip metrics
    df["trip_duration_mins"] = (
        (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds() / 60
    ).round(2)
    df["avg_speed_mph"] = (
        df["trip_distance"] / (df["trip_duration_mins"] / 60)
    ).replace([float("inf"), float("-inf")], None).round(2)
    df["tip_pct"] = (
        (df["tip_amount"] / df["fare_amount"].replace(0, None)) * 100
    ).fillna(0).round(2)

    # Quality filters
    before = len(df)
    df = df[
        df["trip_distance"].between(0.1, 200)      &
        df["fare_amount"].between(2.5, 1000)       &
        df["trip_duration_mins"].between(0.5, 420) &
        df["passenger_count"].between(1, 6)        &
        (df["dropoff_datetime"] > df["pickup_datetime"])
    ]
    log.info(f"After quality filters: {len(df):,} rows (removed {before - len(df):,})")

    # Payment type labels
    payment_map = {1: "credit_card", 2: "cash", 3: "no_charge", 4: "dispute"}
    df["payment_type_desc"] = df["payment_type"].map(payment_map).fillna("unknown")

    # Save
    out_path = OUTPUT_DIR / "trips.parquet"
    df.to_parquet(out_path, index=False)
    log.info(f"Saved transformed data to: {out_path}")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: LOAD
# ─────────────────────────────────────────────────────────────────────────────
def load(df: pd.DataFrame):
    log.info("Loading into DuckDB...")
    conn = duckdb.connect(str(DB_PATH))

    conn.execute("CREATE SCHEMA IF NOT EXISTS nyc_tlc")
    conn.execute("DROP TABLE IF EXISTS nyc_tlc.trips")
    conn.execute("CREATE TABLE nyc_tlc.trips AS SELECT * FROM df")

    row_count = conn.execute("SELECT COUNT(*) FROM nyc_tlc.trips").fetchone()[0]
    log.info(f"Loaded {row_count:,} rows into nyc_tlc.trips")

    # Analytics views
    conn.execute("""
        CREATE OR REPLACE VIEW nyc_tlc.v_hourly_trips AS
        SELECT pickup_hour, is_weekend,
            COUNT(*) AS trip_count,
            ROUND(AVG(fare_amount), 2) AS avg_fare,
            ROUND(AVG(tip_pct), 2) AS avg_tip_pct,
            ROUND(AVG(trip_duration_mins), 2) AS avg_duration_mins
        FROM nyc_tlc.trips GROUP BY 1, 2 ORDER BY 1
    """)
    conn.execute("""
        CREATE OR REPLACE VIEW nyc_tlc.v_daily_revenue AS
        SELECT CAST(pickup_datetime AS DATE) AS trip_date,
            COUNT(*) AS total_trips,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(fare_amount), 2) AS avg_fare
        FROM nyc_tlc.trips GROUP BY 1 ORDER BY 1
    """)
    conn.execute("""
        CREATE OR REPLACE VIEW nyc_tlc.v_payment_summary AS
        SELECT payment_type_desc,
            COUNT(*) AS trip_count,
            ROUND(AVG(tip_pct), 2) AS avg_tip_pct,
            ROUND(SUM(total_amount), 2) AS total_revenue
        FROM nyc_tlc.trips GROUP BY 1 ORDER BY 2 DESC
    """)

    log.info("Analytics views created")

    print("\n--- Hourly Trip Patterns (top 5) ---")
    print(conn.execute("SELECT * FROM nyc_tlc.v_hourly_trips LIMIT 5").df().to_string(index=False))
    print("\n--- Payment Summary ---")
    print(conn.execute("SELECT * FROM nyc_tlc.v_payment_summary").df().to_string(index=False))
    print("\n--- Daily Revenue (first 5 days) ---")
    print(conn.execute("SELECT * FROM nyc_tlc.v_daily_revenue LIMIT 5").df().to_string(index=False))

    conn.close()
    log.info(f"DuckDB warehouse saved: {DB_PATH}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=" * 55)
    log.info("  NYC TLC ETL Pipeline  (Pandas + DuckDB, no Java!)")
    log.info("=" * 55)

    log.info("\n[ STEP 1 ] EXTRACT")
    extract()

    log.info("\n[ STEP 2 ] TRANSFORM")
    df = transform()

    log.info("\n[ STEP 3 ] LOAD")
    load(df)

    log.info("\nPipeline complete! Warehouse at: data/warehouse.duckdb")
