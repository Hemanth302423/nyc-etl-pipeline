# NYC TLC Yellow Taxi ETL Pipeline 🚕

A production-style ETL pipeline that ingests, transforms, and loads NYC Yellow Taxi trip data into a local DuckDB data warehouse — built with **Claude Desktop** as an AI co-pilot.

---

## Architecture

```
NYC TLC Open Data (Parquet)
        │
        ▼
[ EXTRACT ]  Python + requests — streaming download
        │
        ▼
[ TRANSFORM ]  Pandas — clean, enrich, validate
        │
        ▼
[ LOAD ]  DuckDB local warehouse + analytics views
        │
        ▼
[ QUERY ]  query_warehouse.py — SQL analytics
```

---

## Stack

| Layer | Tool |
|---|---|
| Language | Python 3.12 |
| Transformation | Pandas |
| Warehouse | DuckDB |
| Data Source | NYC TLC Open Data (Jan 2023) |
| AI Co-pilot | Claude Desktop |

---

## Quickstart

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/nyc-etl-pipeline.git
cd nyc-etl-pipeline
```

### 2. Create virtual environment
```bash
# Mac/Linux
python -m venv venv
source venv/bin/activate

# Windows (PowerShell)
py -3.12 -m venv venv
venv\Scripts\activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Run the pipeline
```bash
python run_etl.py
```

The script will:
- Download Jan 2023 Yellow Taxi data (~50 MB)
- Clean and transform it with Pandas
- Load it into `data/warehouse.duckdb`
- Print a preview of analytics views

### 5. Query the warehouse
```bash
python query_warehouse.py
```

---

## Project Structure

```
nyc-etl-pipeline/
├── run_etl.py                ← End-to-end pipeline runner (start here)
├── query_warehouse.py        ← SQL queries on the DuckDB warehouse
├── requirements.txt          ← Python dependencies
├── .gitignore
├── AI_PROMPTS.md             ← Prompts used with Claude Desktop
├── README.md
├── extractors/
│   └── nyc_tlc_extractor.py  ← Download logic with retry + validation
├── transformers/
│   └── trip_transformer.py   ← PySpark transformer (advanced reference)
├── loaders/
│   └── duckdb_loader.py      ← DuckDB loader with upsert + views
├── dags/
│   └── etl_pipeline.py       ← Prefect orchestration (advanced reference)
└── tests/
    └── test_transformer.py   ← Unit tests
```

---

## Transformations Applied

| Step | Detail |
|---|---|
| Column rename | Standardised to snake_case |
| Null handling | Dropped rows missing critical fields |
| Type casting | Enforced correct dtypes for all columns |
| Time features | year, month, day, hour, day-of-week, is_weekend, time_of_day |
| Trip metrics | duration (mins), avg speed (mph), tip percentage |
| Quality filters | Removed outliers: distance, fare, speed, duration, passenger count |
| Payment labels | Mapped integer codes to readable labels |

---

## Analytics Views in DuckDB

| View | Description |
|---|---|
| `nyc_tlc.v_hourly_trips` | Trip count, avg fare, avg tip by hour |
| `nyc_tlc.v_daily_revenue` | Daily total trips and revenue |
| `nyc_tlc.v_payment_summary` | Breakdown by payment method |

### Sample queries
```python
import duckdb
conn = duckdb.connect("data/warehouse.duckdb")

# Busiest hours
conn.execute("SELECT * FROM nyc_tlc.v_hourly_trips ORDER BY trip_count DESC").df()

# Daily revenue
conn.execute("SELECT * FROM nyc_tlc.v_daily_revenue").df()

# Payment breakdown
conn.execute("SELECT * FROM nyc_tlc.v_payment_summary").df()
```

---

## Built with Claude Desktop 🤖

This pipeline was built using **Claude Desktop** as an AI co-pilot throughout development.
See [`AI_PROMPTS.md`](./AI_PROMPTS.md) for the exact prompts used at each stage.

---

## Data Source

[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — publicly available open data released by the NYC Taxi & Limousine Commission.
