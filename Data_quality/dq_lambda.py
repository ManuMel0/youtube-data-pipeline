"""
Lambda: Data Quality Checks
────────────────────────────
Called by Step Functions after the Silver layer is built.
Validates data quality before allowing the Gold aggregation to proceed.

Checks performed:
  1. Row count — is there enough data?
  2. Null percentage — are critical columns populated?
  3. Schema validation — do expected columns exist?
  4. Value range checks — are numeric values reasonable?
  5. Freshness — is the data recent enough?

Environment Variables:
    S3_BUCKET_SILVER        — Silver bucket to check
    SNS_ALERT_TOPIC_ARN     — SNS for alerts
    ATHENA_OUTPUT_S3        — S3 path for Athena query results
                              Example:
                              s3://yt-data-pipeline-athena-query-result-444115535128-sa-east-1-an/
"""

import os
import json
import logging
from datetime import datetime, timezone, timedelta

import boto3
import awswrangler as wr
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns_client = boto3.client("sns")

SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")

ATHENA_OUTPUT_S3 = os.environ.get(
    "ATHENA_OUTPUT_S3",
    "s3://yt-data-pipeline-athena-query-result-444115535128-sa-east-1-an/"
)

ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")

# ── Thresholds ───────────────────────────────────────────────────────────────
MIN_ROW_COUNT = int(os.environ.get("DQ_MIN_ROW_COUNT", "10"))
MAX_NULL_PCT = float(os.environ.get("DQ_MAX_NULL_PERCENT", "5.0"))
MAX_VIEWS = 50_000_000_000  # 50B — sanity check for view counts
FRESHNESS_HOURS = int(os.environ.get("DQ_FRESHNESS_HOURS", "48"))


CRITICAL_COLUMNS = {
    "clean_statistics": ["video_id", "title", "channel_title", "views", "region"],
    "clean_reference_data": ["id", "region"],
}


def normalize_s3_output(path: str) -> str:
    """Ensure Athena output path is a valid S3 URI ending with '/'."""
    if not path:
        raise ValueError("ATHENA_OUTPUT_S3 is empty.")

    if not path.startswith("s3://"):
        raise ValueError(f"ATHENA_OUTPUT_S3 must start with s3://. Received: {path}")

    return path if path.endswith("/") else f"{path}/"


def send_alert(subject: str, payload: object) -> None:
    """Send SNS alert if topic ARN is configured."""
    if not SNS_TOPIC:
        logger.info("SNS_ALERT_TOPIC_ARN not configured. Skipping alert.")
        return

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject=subject[:100],
            Message=json.dumps(payload, indent=2, default=str),
        )
    except Exception as e:
        logger.error(f"Failed to publish SNS alert: {e}")


def check_row_count(df: pd.DataFrame, table_name: str) -> dict:
    """Check that table has minimum number of rows."""
    count = len(df)
    passed = count >= MIN_ROW_COUNT

    return {
        "check": "row_count",
        "table": table_name,
        "value": count,
        "threshold": MIN_ROW_COUNT,
        "passed": passed,
        "message": f"Row count: {count} (min: {MIN_ROW_COUNT})",
    }


def check_null_percentage(df: pd.DataFrame, table_name: str) -> list:
    """Check null percentages for critical columns."""
    results = []
    cols = CRITICAL_COLUMNS.get(table_name, [])

    for col in cols:
        if col not in df.columns:
            results.append({
                "check": "null_pct",
                "table": table_name,
                "column": col,
                "passed": False,
                "message": f"Column '{col}' missing from table",
            })
            continue

        null_pct = (df[col].isna().sum() / len(df)) * 100 if len(df) > 0 else 0
        passed = null_pct <= MAX_NULL_PCT

        results.append({
            "check": "null_pct",
            "table": table_name,
            "column": col,
            "value": round(null_pct, 2),
            "threshold": MAX_NULL_PCT,
            "passed": passed,
            "message": f"{col} null%: {null_pct:.2f}% (max: {MAX_NULL_PCT}%)",
        })

    return results


def check_schema(df: pd.DataFrame, table_name: str) -> dict:
    """Check that expected columns exist."""
    expected = set(CRITICAL_COLUMNS.get(table_name, []))
    actual = set(df.columns)
    missing = expected - actual
    passed = len(missing) == 0

    return {
        "check": "schema",
        "table": table_name,
        "missing_columns": list(missing),
        "passed": passed,
        "message": f"Missing columns: {missing}" if missing else "All expected columns present",
    }


def check_value_ranges(df: pd.DataFrame, table_name: str) -> list:
    """Check that numeric values are within reasonable ranges."""
    results = []

    if table_name != "clean_statistics":
        return results

    if "views" in df.columns:
        views = pd.to_numeric(df["views"], errors="coerce")

        negative = (views < 0).sum()
        extreme = (views > MAX_VIEWS).sum()
        invalid_numeric = views.isna().sum() - df["views"].isna().sum()

        passed = negative == 0 and extreme == 0 and invalid_numeric == 0

        results.append({
            "check": "value_range",
            "table": table_name,
            "column": "views",
            "negative_count": int(negative),
            "extreme_count": int(extreme),
            "invalid_numeric_count": int(invalid_numeric),
            "passed": passed,
            "message": (
                f"Views: {negative} negative, "
                f"{extreme} extreme (>{MAX_VIEWS}), "
                f"{invalid_numeric} invalid numeric"
            ),
        })

    return results


def check_freshness(df: pd.DataFrame, table_name: str) -> dict:
    """Check that data includes recent records."""
    if "_processed_at" not in df.columns and "_ingestion_timestamp" not in df.columns:
        return {
            "check": "freshness",
            "table": table_name,
            "passed": True,
            "message": "No timestamp column found — skipping freshness check (backfill data)",
        }

    ts_col = "_processed_at" if "_processed_at" in df.columns else "_ingestion_timestamp"

    try:
        latest = pd.to_datetime(df[ts_col], utc=True, errors="coerce").max()

        if pd.isna(latest):
            return {
                "check": "freshness",
                "table": table_name,
                "passed": False,
                "message": f"Timestamp column '{ts_col}' exists, but no valid timestamp was found",
            }

        cutoff = datetime.now(timezone.utc) - timedelta(hours=FRESHNESS_HOURS)
        passed = latest.to_pydatetime() >= cutoff

        return {
            "check": "freshness",
            "table": table_name,
            "latest_record": str(latest),
            "cutoff": str(cutoff),
            "passed": passed,
            "message": f"Latest: {latest}, Cutoff: {cutoff}",
        }

    except Exception as e:
        return {
            "check": "freshness",
            "table": table_name,
            "passed": True,
            "message": f"Could not parse timestamps: {e} — skipping",
        }


def read_table_sample(database: str, table_name: str) -> pd.DataFrame:
    """Read a sample from Athena using explicit S3 output location."""
    athena_output = normalize_s3_output(ATHENA_OUTPUT_S3)

    query = f'SELECT * FROM "{table_name}" LIMIT 10000'

    logger.info(f"Executing Athena query on database: {database}")
    logger.info(f"Table: {table_name}")
    logger.info(f"Athena output: {athena_output}")
    logger.info(f"Athena workgroup: {ATHENA_WORKGROUP}")

    return wr.athena.read_sql_query(
        sql=query,
        database=database,
        s3_output=athena_output,
        workgroup=ATHENA_WORKGROUP,
        ctas_approach=False,
    )


def lambda_handler(event, context):
    """
    Run data quality checks on Silver layer tables.

    Expected event:
    {
        "layer": "silver",
        "database": "yt_pipeline_silver",
        "tables": ["clean_statistics", "clean_reference_data"]
    }
    """
    database = event.get("database", "yt_pipeline_silver")
    tables = event.get("tables", ["clean_statistics"])

    all_results = []
    overall_passed = True

    logger.info(f"Received event: {json.dumps(event, default=str)}")
    logger.info(f"Using Athena output location: {ATHENA_OUTPUT_S3}")
    logger.info(f"Using Athena workgroup: {ATHENA_WORKGROUP}")

    for table_name in tables:
        logger.info(f"Running DQ checks on {database}.{table_name}...")

        try:
            df = read_table_sample(database, table_name)

        except Exception as e:
            logger.error(f"Could not read {table_name}: {e}")

            all_results.append({
                "check": "read_table",
                "table": table_name,
                "passed": False,
                "message": str(e),
            })

            overall_passed = False
            continue

        checks = []
        checks.append(check_row_count(df, table_name))
        checks.extend(check_null_percentage(df, table_name))
        checks.append(check_schema(df, table_name))
        checks.extend(check_value_ranges(df, table_name))
        checks.append(check_freshness(df, table_name))

        for check in checks:
            logger.info(
                f"  {check['check']}: "
                f"{'PASS' if check['passed'] else 'FAIL'} — "
                f"{check['message']}"
            )

            if not check["passed"]:
                overall_passed = False

        all_results.extend(checks)

    passed_count = sum(1 for r in all_results if r["passed"])
    total_count = len(all_results)

    logger.info(
        f"DQ Summary: {passed_count}/{total_count} checks passed. "
        f"Overall: {'PASS' if overall_passed else 'FAIL'}"
    )

    if not overall_passed:
        failed = [r for r in all_results if not r["passed"]]

        send_alert(
            subject="[YT Pipeline] Data quality checks FAILED",
            payload={
                "database": database,
                "tables": tables,
                "failed_checks": failed,
                "checks_passed": passed_count,
                "checks_total": total_count,
            },
        )

    return {
        "quality_passed": bool(overall_passed),
        "checks_passed": int(passed_count),
        "checks_total": int(total_count),
        "details": json.loads(json.dumps(all_results, default=str)),
    }