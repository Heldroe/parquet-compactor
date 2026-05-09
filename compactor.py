from datetime import datetime, timedelta, timezone
import os
import re
import sys
import time

import boto3
import duckdb


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

raw_endpoint = os.getenv("S3_ENDPOINT")
S3_ENDPOINT = re.sub(r"^https?://", "", raw_endpoint).rstrip("/") if raw_endpoint else None
S3_REGION = os.getenv("S3_REGION")
BUCKET = os.getenv("BUCKET_NAME", "logs-heormv0t")

# MODE: "hourly" or "daily" — must be set explicitly.
# Hourly cron: MODE=hourly  (runs at xx:15, compacts the just-completed hour)
# Daily  cron: MODE=daily   (runs at 01:30, compacts all hours of yesterday)
MODE = os.getenv("MODE", "").strip().lower()
if MODE not in ("hourly", "daily"):
    print(f"ERROR: MODE must be 'hourly' or 'daily', got '{MODE}'. Set the MODE environment variable.")
    sys.exit(1)

CATEGORIES_ENV = os.getenv("CATEGORIES", "containers")
CATEGORIES = [c.strip() for c in CATEGORIES_ENV.split(",") if c.strip()]

# Delete source files after compaction in either mode:
#   hourly mode -> deletes raw files after each hour is compacted
#   daily  mode -> deletes hourly files after each day is merged
DELETE_FILES = os.getenv("DELETE_FILES", "false").lower() in ("true", "1", "yes")

HOURLY_PREFIX = os.getenv("HOURLY_PREFIX", "hourly").strip("/")
DAILY_PREFIX = os.getenv("DAILY_PREFIX", "daily").strip("/")

# Separate watermark paths for each mode.
WATERMARK_ENABLED = os.getenv("WATERMARK_ENABLED", "true").lower() in ("true", "1", "yes")
HOURLY_WATERMARK_PATH = os.getenv("HOURLY_WATERMARK_PATH", "/watermarks/hourly.csv")
DAILY_WATERMARK_PATH  = os.getenv("DAILY_WATERMARK_PATH",  "/watermarks/daily.csv")

# When WATERMARK_ENABLED=false, fallback look-back windows.
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "1"))   # for hourly mode
LOOKBACK_DAYS  = int(os.getenv("LOOKBACK_DAYS",  "1"))   # for daily  mode


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

SCRIPT_START = time.monotonic()


def ts():
    """Current UTC timestamp prefix for log lines."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def log(msg, indent=0):
    prefix = "  " * indent
    print(f"[{ts()}] {prefix}{msg}")


def log_section(title):
    print(f"\n[{ts()}] {'─' * 60}")
    print(f"[{ts()}] {title}")
    print(f"[{ts()}] {'─' * 60}")


def elapsed(since: float) -> str:
    secs = time.monotonic() - since
    if secs < 60:
        return f"{secs:.1f}s"
    return f"{secs / 60:.1f}m"


# ---------------------------------------------------------------------------
# S3 / boto3
# ---------------------------------------------------------------------------

s3_client = None
if DELETE_FILES:
    boto3_kwargs = {}
    if raw_endpoint:
        boto3_kwargs["endpoint_url"] = f"https://{S3_ENDPOINT}"
    if S3_REGION:
        boto3_kwargs["region_name"] = S3_REGION

    log("File deletion is ENABLED. Initializing boto3 client...")
    s3_client = boto3.client("s3", **boto3_kwargs)
else:
    log("File deletion is DISABLED. Set DELETE_FILES=true to enable.")


# ---------------------------------------------------------------------------
# DuckDB setup
# ---------------------------------------------------------------------------

def setup_duckdb():
    log_section("Initializing DuckDB")
    con = duckdb.connect(":memory:")
    con.execute("LOAD httpfs;")
    con.execute("LOAD aws;")
    con.execute("SET threads = 2;")
    con.execute("SET http_retries = 3;")
    con.execute("SET http_retry_wait_ms = 500;")
    con.execute("SET http_retry_backoff = 2;")

    if S3_ENDPOINT:
        con.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
        con.execute("SET s3_url_style='vhost';")
    if S3_REGION:
        con.execute(f"SET s3_region='{S3_REGION}';")

    con.execute("CALL load_aws_credentials();")
    log("DuckDB ready.")
    return con


# ---------------------------------------------------------------------------
# Watermark
# ---------------------------------------------------------------------------

def _is_not_found_error(msg: str) -> bool:
    return any(k in msg for k in ("404", "NoSuchKey", "No files found"))


def read_watermark(con, watermark_path: str) -> datetime | None:
    """
    Read a timestamp from an S3 CSV watermark file.
    Returns a UTC-aware datetime snapped to the nearest hour (hourly mode)
    or day (daily mode), or None if the file does not exist yet.
    Raises on any unexpected error.
    """
    full_path = f"s3://{BUCKET}{watermark_path}"
    try:
        res = con.execute(
            f"SELECT column0 FROM read_csv_auto('{full_path}', header=false)"
        ).fetchone()

        val = res[0]
        raw_time = (
            datetime.strptime(val, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            if isinstance(val, str)
            else val.replace(tzinfo=timezone.utc)
        )

        if MODE == "hourly":
            snapped = raw_time.replace(minute=0, second=0, microsecond=0)
            boundary_label = "hour"
        else:
            snapped = raw_time.replace(hour=0, minute=0, second=0, microsecond=0)
            boundary_label = "day"

        if raw_time != snapped:
            log(
                f"WARNING: Watermark {raw_time} was not on a {boundary_label} boundary. "
                f"Snapped to {snapped}."
            )
        return snapped

    except Exception as e:
        if not _is_not_found_error(str(e)):
            log(f"ERROR: Fatal error reading watermark '{watermark_path}': {e}")
            raise
        return None  # File does not exist yet — caller handles bootstrap.


def write_watermark(con, watermark_path: str, new_time: datetime):
    full_path = f"s3://{BUCKET}{watermark_path}"
    new_time_str = new_time.strftime("%Y-%m-%d %H:%M:%S")
    con.execute(
        f"COPY (SELECT '{new_time_str}') TO '{full_path}' (FORMAT CSV, HEADER FALSE);"
    )
    log(f"Watermark updated to {new_time_str}", indent=1)


def bootstrap_hourly_watermark(con) -> datetime | None:
    """Discover the oldest raw file and return its hour as the starting watermark."""
    log("Watermark not found. Auto-discovering oldest raw logs in S3...")
    first_file = con.execute(f"""
        SELECT file FROM glob('s3://{BUCKET}/raw/*/*/*/*/*/*.parquet')
        ORDER BY file ASC LIMIT 1
    """).fetchone()

    if not first_file:
        log("No raw logs found in the bucket yet. Nothing to do.")
        return None

    path = first_file[0]
    log(f"Oldest file found: {path}", indent=1)
    match = re.search(r"year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})", path)
    if not match:
        raise ValueError(f"Failed to parse Hive partitions from path: {path}")

    y, m, d, h = match.groups()
    discovered = datetime(int(y), int(m), int(d), int(h), tzinfo=timezone.utc)
    log(f"Bootstrapping from auto-discovered time: {discovered}", indent=1)
    return discovered


def bootstrap_daily_watermark(con) -> datetime | None:
    """Discover the oldest hourly-compacted file and return its day as the starting watermark."""
    log("Watermark not found. Auto-discovering oldest hourly-compacted files in S3...")
    first_file = con.execute(f"""
        SELECT file FROM glob('s3://{BUCKET}/{HOURLY_PREFIX}/*/*/*/*/*/*.parquet')
        ORDER BY file ASC LIMIT 1
    """).fetchone()

    if not first_file:
        log("No hourly-compacted files found in the bucket yet. Nothing to do.")
        return None

    path = first_file[0]
    log(f"Oldest file found: {path}", indent=1)
    match = re.search(r"year=(\d{4})/month=(\d{2})/day=(\d{2})", path)
    if not match:
        raise ValueError(f"Failed to parse Hive partitions from path: {path}")

    y, m, d = match.groups()
    discovered = datetime(int(y), int(m), int(d), tzinfo=timezone.utc)
    log(f"Bootstrapping from auto-discovered date: {discovered.date()}", indent=1)
    return discovered


# ---------------------------------------------------------------------------
# Deletion
# ---------------------------------------------------------------------------

def delete_raw_files_for_prefix(bucket, prefix):
    """Deletes all objects under a given S3 prefix in batches of 1000."""
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    deleted_count = 0
    for page in pages:
        if "Contents" in page:
            objects_to_delete = [{"Key": obj["Key"]} for obj in page["Contents"]]
            s3_client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": objects_to_delete, "Quiet": True},
            )
            deleted_count += len(objects_to_delete)

    return deleted_count


# ---------------------------------------------------------------------------
# Hourly compaction
# ---------------------------------------------------------------------------

def compact_hour(con, processing_time: datetime, timing_stats: dict):
    y = processing_time.strftime("%Y")
    m = processing_time.strftime("%m")
    d = processing_time.strftime("%d")
    h = processing_time.strftime("%H")

    log(f"Compacting hour {processing_time.strftime('%Y-%m-%d %H:00')} UTC")

    for category in CATEGORIES:
        raw_path = (
            f"s3://{BUCKET}/raw/{category}"
            f"/year={y}/month={m}/day={d}/hour={h}/*.parquet"
        )
        compacted_path = (
            f"s3://{BUCKET}/{HOURLY_PREFIX}/{category}"
            f"/year={y}/month={m}/day={d}/hour={h}/data.parquet"
        )

        try:
            t0 = time.monotonic()
            con.execute(f"""
                COPY (
                    SELECT * FROM read_parquet(
                        '{raw_path}',
                        hive_partitioning=true,
                        union_by_name=true
                    )
                ) TO '{compacted_path}' (FORMAT PARQUET);
            """)
            timing_stats["compaction"] += time.monotonic() - t0
            log(f"[{category}] Written -> {compacted_path} ({elapsed(t0)})", indent=1)

            if DELETE_FILES:
                raw_prefix = f"raw/{category}/year={y}/month={m}/day={d}/hour={h}/"
                t1 = time.monotonic()
                deleted = delete_raw_files_for_prefix(BUCKET, raw_prefix)
                timing_stats["deletion"] += time.monotonic() - t1
                log(f"[{category}] Deleted {deleted} raw file(s) ({elapsed(t1)})", indent=1)
            else:
                log(f"[{category}] Raw deletion skipped (disabled).", indent=1)

        except Exception as e:
            if "No files found" in str(e):
                log(f"[{category}] No raw files found for this hour. Skipping.", indent=1)
            else:
                raise


# ---------------------------------------------------------------------------
# Daily compaction
# ---------------------------------------------------------------------------

def compact_day(con, day: datetime, timing_stats: dict):
    """
    Merges all hourly-compacted parquet files for a given UTC day into a single
    daily file at:
      s3://<BUCKET>/<DAILY_PREFIX>/<category>/year=Y/month=M/day=D/hour=0/data.parquet
    """
    y = day.strftime("%Y")
    m = day.strftime("%m")
    d = day.strftime("%d")

    log(f"Compacting day {day.strftime('%Y-%m-%d')} UTC")

    for category in CATEGORIES:
        hourly_glob = (
            f"s3://{BUCKET}/{HOURLY_PREFIX}/{category}"
            f"/year={y}/month={m}/day={d}/hour=*/*.parquet"
        )
        daily_path = (
            f"s3://{BUCKET}/{DAILY_PREFIX}/{category}"
            f"/year={y}/month={m}/day={d}/hour=0/data.parquet"
        )

        try:
            t0 = time.monotonic()
            con.execute(f"""
                COPY (
                    SELECT * FROM read_parquet(
                        '{hourly_glob}',
                        hive_partitioning=true,
                        union_by_name=true
                    )
                ) TO '{daily_path}' (FORMAT PARQUET);
            """)
            timing_stats["daily_compaction"] += time.monotonic() - t0
            log(f"[{category}] Written -> {daily_path} ({elapsed(t0)})", indent=1)

            if DELETE_FILES:
                hourly_prefix = f"{HOURLY_PREFIX}/{category}/year={y}/month={m}/day={d}/"
                t1 = time.monotonic()
                deleted = delete_raw_files_for_prefix(BUCKET, hourly_prefix)
                timing_stats["hourly_deletion"] += time.monotonic() - t1
                log(f"[{category}] Deleted {deleted} hourly file(s) ({elapsed(t1)})", indent=1)
            else:
                log(f"[{category}] Hourly file deletion skipped (disabled).", indent=1)

        except Exception as e:
            if "No files found" in str(e):
                log(
                    f"[{category}] No hourly files found for {day.strftime('%Y-%m-%d')}. Skipping.",
                    indent=1,
                )
            else:
                raise


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    total_start = time.monotonic()

    timing_stats = {
        "compaction": 0.0,
        "deletion": 0.0,
        "daily_compaction": 0.0,
        "hourly_deletion": 0.0,
    }

    con = setup_duckdb()

    now = datetime.now(timezone.utc)

    # =========================================================================
    if MODE == "hourly":
    # =========================================================================

        log_section("Hourly Compaction")
        log(f"Mode            : hourly")
        log(f"Hourly prefix   : {HOURLY_PREFIX}/")
        log(f"Watermark       : {'enabled' if WATERMARK_ENABLED else 'disabled'}")
        log(f"Delete files    : {'enabled' if DELETE_FILES else 'disabled'}")

        current_hour = now.replace(minute=0, second=0, microsecond=0)

        if WATERMARK_ENABLED:
            watermark = read_watermark(con, HOURLY_WATERMARK_PATH)
            if watermark is None:
                watermark = bootstrap_hourly_watermark(con)
            if watermark is None:
                log("Exiting gracefully — no data found.")
                return
        else:
            watermark = current_hour - timedelta(hours=LOOKBACK_HOURS)
            log(f"Watermark disabled. Processing last {LOOKBACK_HOURS} hour(s) from {watermark.strftime('%Y-%m-%d %H:%M')} UTC.")

        if watermark >= current_hour:
            log("Everything is up to date. Nothing to compact.")
            return

        log(f"Processing range: [{watermark.strftime('%Y-%m-%d %H:%M')} — {current_hour.strftime('%Y-%m-%d %H:%M')}) UTC")

        processing_time = watermark
        while processing_time < current_hour:
            compact_hour(con, processing_time, timing_stats)
            processing_time += timedelta(hours=1)
            if WATERMARK_ENABLED:
                write_watermark(con, HOURLY_WATERMARK_PATH, processing_time)

    # =========================================================================
    elif MODE == "daily":
    # =========================================================================

        log_section("Daily Compaction")
        log(f"Mode            : daily")
        log(f"Hourly prefix   : {HOURLY_PREFIX}/")
        log(f"Daily prefix    : {DAILY_PREFIX}/")
        log(f"Watermark       : {'enabled' if WATERMARK_ENABLED else 'disabled'}")
        log(f"Delete files    : {'enabled' if DELETE_FILES else 'disabled'}")

        # Daily runs at 01:30; "yesterday" is always the day before today UTC.
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)

        if WATERMARK_ENABLED:
            watermark = read_watermark(con, DAILY_WATERMARK_PATH)
            if watermark is None:
                watermark = bootstrap_daily_watermark(con)
            if watermark is None:
                log("Exiting gracefully — no data found.")
                return
        else:
            watermark = today - timedelta(days=LOOKBACK_DAYS)
            log(f"Watermark disabled. Processing last {LOOKBACK_DAYS} day(s) from {watermark.strftime('%Y-%m-%d')} UTC.")

        if watermark >= today:
            log("Everything is up to date. Nothing to compact.")
            return

        log(f"Processing range: [{watermark.strftime('%Y-%m-%d')} — {today.strftime('%Y-%m-%d')}) UTC")

        processing_day = watermark
        while processing_day < today:
            compact_day(con, processing_day, timing_stats)
            processing_day += timedelta(days=1)
            if WATERMARK_ENABLED:
                write_watermark(con, DAILY_WATERMARK_PATH, processing_day)

    # ---- Summary ------------------------------------------------------------

    log_section("Summary")
    total_elapsed = time.monotonic() - total_start

    if MODE == "hourly":
        log(f"Compaction time  : {timing_stats['compaction']:.1f}s")
        if DELETE_FILES:
            log(f"Deletion time    : {timing_stats['deletion']:.1f}s")
    else:
        log(f"Daily merge time : {timing_stats['daily_compaction']:.1f}s")
        if DELETE_FILES:
            log(f"Deletion time    : {timing_stats['hourly_deletion']:.1f}s")

    log(f"Total time       : {total_elapsed:.1f}s")
    log("Compaction completed successfully.")


if __name__ == "__main__":
    main()
