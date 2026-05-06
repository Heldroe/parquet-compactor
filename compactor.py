from datetime import datetime, timedelta, timezone
import os
import re

import duckdb


raw_endpoint = os.getenv("S3_ENDPOINT")
S3_ENDPOINT = re.sub(r"^https?://", "", raw_endpoint).rstrip("/") if raw_endpoint else None
S3_REGION = os.getenv("S3_REGION")
BUCKET = os.getenv("BUCKET_NAME", "logs-heormv0t")
WATERMARK_PATH = os.getenv("WATERMARK_PATH", "/watermark.csv")

CATEGORIES_ENV = os.getenv("CATEGORIES", "containers")
CATEGORIES = [c.strip() for c in CATEGORIES_ENV.split(",") if c.strip()]

def setup_duckdb():
    print("Initializing DuckDB and loading pre-installed extensions...")
    con = duckdb.connect(':memory:')
    con.execute("LOAD httpfs;")
    con.execute("LOAD aws;")

    if S3_ENDPOINT:
        con.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
        con.execute("SET s3_url_style='vhost';")
    if S3_REGION:
        con.execute(f"SET s3_region='{S3_REGION}';")

    con.execute("CALL load_aws_credentials();")
    return con

def get_current_watermark(con):
    full_watermark_path = f"s3://{BUCKET}{WATERMARK_PATH}"
    try:
        res = con.execute(f"SELECT column0 FROM read_csv_auto('{full_watermark_path}', header=false)").fetchone()

        # Parse the string and explicitly make it a timezone-aware UTC datetime
        raw_time = datetime.strptime(res[0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        snapped_time = raw_time.replace(minute=0, second=0, microsecond=0)

        if raw_time != snapped_time:
            print(f"⚠️ Warning: Watermark was not on an hour boundary ({raw_time}). Snapped to {snapped_time}.")

        return snapped_time

    except Exception as e:
        error_msg = str(e)
        # Check if the error is genuinely because the file doesn't exist (404 / NoSuchKey)
        if "404" not in error_msg and "NoSuchKey" not in error_msg and "No files found" not in error_msg:
            print(f"❌ Fatal Error accessing watermark file: {error_msg}")
            raise e

        print("🔍 Watermark not found. Auto-discovering the oldest raw logs in S3...")

        query = f"""
            SELECT file
            FROM glob('s3://{BUCKET}/raw/*/*/*/*/*/*.parquet')
            ORDER BY file ASC
            LIMIT 1
        """
        # If auto-discovery fails due to credentials, this will natively raise the 403 error
        first_file = con.execute(query).fetchone()

        if not first_file:
            print("No raw logs found in the bucket yet. Nothing to bootstrap.")
            return None

        path = first_file[0]
        print(f"Oldest file found: {path}")

        match = re.search(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})", path)
        if match:
            y, m, d, h = match.groups()
            discovered_time = datetime(int(y), int(m), int(d), int(h), tzinfo=timezone.utc)
            print(f"🚀 Bootstrapping compaction from auto-discovered time: {discovered_time}")
            return discovered_time
        else:
            raise ValueError(f"Failed to parse Hive partitions from path: {path}")

def update_watermark(con, new_time_str):
    full_watermark_path = f"s3://{BUCKET}{WATERMARK_PATH}"
    con.execute(f"COPY (SELECT '{new_time_str}') TO '{full_watermark_path}' (FORMAT CSV);")
    print(f"✅ Watermark updated to {new_time_str}")

def main():
    con = setup_duckdb()
    current_watermark = get_current_watermark(con)

    if current_watermark is None:
        print("Exiting gracefully.")
        return

    now = datetime.now(timezone.utc)
    current_hour_boundary = now.replace(minute=0, second=0, microsecond=0)

    if current_watermark >= current_hour_boundary:
        print("Everything is up to date. Exiting.")
        return

    processing_time = current_watermark

    while processing_time < current_hour_boundary:
        y = processing_time.strftime('%Y')
        m = processing_time.strftime('%m')
        d = processing_time.strftime('%d')
        h = processing_time.strftime('%H')

        print(f"⏳ Compacting hour {processing_time.strftime('%Y-%m-%d %H:00:00')} UTC...")

        for category in CATEGORIES:
            raw_path = f"s3://{BUCKET}/raw/{category}/year={y}/month={m}/day={d}/hour={h}/*.parquet"
            compacted_path = f"s3://{BUCKET}/compacted/{category}/year={y}/month={m}/day={d}/hour={h}/data.parquet"

            try:
                con.execute(f"""
                    COPY (
                        SELECT * FROM read_parquet('{raw_path}', hive_partitioning=true, union_by_name=true)
                    ) TO '{compacted_path}' (FORMAT PARQUET);
                """)
                print(f"  -> [{category}] Successfully wrote {compacted_path}")

            except Exception as e:
                if "No files found" in str(e):
                    print(f"  -> [{category}] No raw files found for this hour. Skipping safely.")
                else:
                    raise e

        processing_time += timedelta(hours=1)
        # Format it back to a standard string for the CSV file
        update_watermark(con, processing_time.strftime("%Y-%m-%d %H:%M:%S"))

    print("🎉 All backlog compacted successfully!")

if __name__ == "__main__":
    main()
