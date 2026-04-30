"""
query_warehouse.py
------------------
Run SQL analytics queries against the DuckDB warehouse
after the ETL pipeline has completed.

Run: python query_warehouse.py
"""

import duckdb

conn = duckdb.connect("data/warehouse.duckdb")

# ── 1. Total trips ────────────────────────────────────────────
print("\n=== Total trips loaded ===")
print(conn.execute("SELECT COUNT(*) AS total_trips FROM nyc_tlc.trips").df())

# ── 2. Busiest hours ──────────────────────────────────────────
print("\n=== Busiest hours of the day ===")
print(conn.execute("""
    SELECT * FROM nyc_tlc.v_hourly_trips
    ORDER BY trip_count DESC
    LIMIT 10
""").df())

# ── 3. Weekday vs weekend ─────────────────────────────────────
print("\n=== Weekday vs Weekend comparison ===")
print(conn.execute("""
    SELECT
        is_weekend,
        COUNT(*)                           AS trip_count,
        ROUND(AVG(fare_amount), 2)         AS avg_fare,
        ROUND(AVG(tip_pct), 2)             AS avg_tip_pct,
        ROUND(AVG(trip_duration_mins), 2)  AS avg_duration_mins
    FROM nyc_tlc.trips
    GROUP BY is_weekend
""").df())

# ── 4. Daily revenue ──────────────────────────────────────────
print("\n=== Daily revenue (first 10 days) ===")
print(conn.execute("SELECT * FROM nyc_tlc.v_daily_revenue LIMIT 10").df())

# ── 5. Payment breakdown ──────────────────────────────────────
print("\n=== Payment method breakdown ===")
print(conn.execute("SELECT * FROM nyc_tlc.v_payment_summary").df())

# ── 6. Top 5 pickup zones ─────────────────────────────────────
print("\n=== Top 5 busiest pickup zones ===")
print(conn.execute("""
    SELECT
        pickup_location_id,
        COUNT(*)                   AS trip_count,
        ROUND(AVG(fare_amount), 2) AS avg_fare
    FROM nyc_tlc.trips
    GROUP BY pickup_location_id
    ORDER BY trip_count DESC
    LIMIT 5
""").df())

# ── 7. Avg speed by time of day ───────────────────────────────
print("\n=== Avg speed by time of day ===")
print(conn.execute("""
    SELECT
        time_of_day,
        COUNT(*)                          AS trip_count,
        ROUND(AVG(avg_speed_mph), 2)      AS avg_speed_mph,
        ROUND(AVG(trip_duration_mins), 2) AS avg_duration_mins
    FROM nyc_tlc.trips
    WHERE avg_speed_mph IS NOT NULL
    GROUP BY time_of_day
    ORDER BY avg_speed_mph DESC
""").df())

conn.close()
print("\nAll queries complete!")
