# Prompts Used with Claude Desktop

This file documents the actual prompts used with Claude Desktop to build this pipeline.
It demonstrates how Claude was used as an AI co-pilot throughout development.

---

## 1. Project Scaffolding
**Prompt:**
> "I need to build a data engineering ETL pipeline for NYC TLC taxi data in Python.
> Help me design the folder structure, choose the right stack (PySpark + DuckDB),
> and generate a simple end-to-end script I can run in under 3 hours."

**Output:** Project structure, stack selection, `run_etl.py`

---

## 2. Extractor
**Prompt:**
> "Write a robust Python extractor for NYC TLC taxi data that supports
> downloading multiple months, validates the file after download,
> and logs progress. Include retry logic."

**Output:** `extractors/nyc_tlc_extractor.py`

---

## 3. PySpark Transformer
**Prompt:**
> "Write a PySpark transformer for NYC taxi data that:
> - Renames columns to snake_case
> - Drops nulls in critical columns
> - Adds time features: year, month, hour, day of week, time_of_day, is_weekend
> - Calculates trip duration in minutes, average speed mph, tip percentage
> - Filters out bad data: zero distance, negative fares, impossible speeds
> - Maps payment_type integer codes to readable labels"

**Output:** `transformers/trip_transformer.py`

---

## 4. DuckDB Loader
**Prompt:**
> "Write a DuckDB loader that reads transformed Parquet files, loads them into
> a local DuckDB database, and creates 3 analytics views:
> hourly trip patterns, daily revenue, and payment summary."

**Output:** `loaders/duckdb_loader.py`

---

## 5. Unit Tests
**Prompt:**
> "Write pytest unit tests for the PySpark transformer. Use a small synthetic
> DataFrame (don't need real data). Test column renaming, null dropping,
> time features, trip metrics, quality filters, and payment enrichment."

**Output:** `tests/test_transformer.py`

---

## 6. Debugging
**Prompt:**
> "My PySpark job is failing with AnalysisException: Column PULocationID not found.
> The raw TLC Parquet uses PULocationID but my transformer expects pickup_location_id.
> How do I safely rename it only if it exists?"

**Output:** Added conditional column rename logic using `df.columns` check.

---

## 7. README
**Prompt:**
> "Write a clean GitHub README for this NYC TLC ETL pipeline project.
> Include architecture overview, setup instructions, how to run it,
> and a section explaining how Claude Desktop was used to build it."

**Output:** `README.md`
