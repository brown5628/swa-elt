from dagster import job, op, repository, schedule, ScheduleDefinition, Nothing, In
from dagster.utils.log import get_dagster_logger
from dagster_gcp.gcs.io_manager import gcs_pickle_io_manager
from dagster_gcp.gcs.resources import gcs_resource
from dagster_gcp.bigquery.resources import bigquery_resource
import dagster_gcp as gcp
import json
import pandas as pd
import feedparser
import datetime

logger = get_dagster_logger()
job_ts = datetime.datetime.utcnow()

@op(
    ins={
        "start": In(Nothing)
    }
)
def get_goodreads_feed(context):
    goodreads_rss="https://www.goodreads.com/review/list_rss/51384411?key=WtDvvUkHkxtfdlRlZRZ5v3AFe4OOq3SGxiQ0xn4NV5oDUDvJ&shelf=%23ALL%23"
    feeds=feedparser.parse(goodreads_rss)

    post_ids = []
    raw_data = []
    pipeline_processed_at = []

    for item in feeds.entries:
        post_ids.append(item.id)
        raw_data.append(json.dumps(item))
        pipeline_processed_at.append(job_ts)

    df_ids = pd.DataFrame(columns=["id"], data=post_ids)
    df_raw_data = pd.DataFrame(columns=["raw_data"], data=raw_data)
    df_pipeline_processed_at = pd.DataFrame(columns=["_pipeline_processed_at"], data=pipeline_processed_at)
    df = pd.concat([df_ids, df_raw_data, df_pipeline_processed_at], axis=1)
    return df

@job(
    resource_defs={
        "gcs": gcs_resource,
        "io_manager": gcs_pickle_io_manager,
        "bigquery": bigquery_resource,
    },
    config={
        "resources": {
            "io_manager": {
                "config": {
                    "gcs_bucket": "small-world-dagster",
                    "gcs_prefix": "dagster-logs",
                }
            },
            "bigquery": {
                "config": {
                    "project": "small-world-analytics"
                }
            }
        },
        "ops": {
            "bq_create_dataset": {
                "config": {
                    "dataset": "raw_goodreads",
                    "exists_ok": True
                }
            },
            "import_df_to_bq": {
                "config": {
                    "destination": "raw_goodreads.goodreads_feed",
                    "load_job_config": {
                        "write_disposition": "WRITE_TRUNCATE"
                    }
                }
            }
        }
    },
)
def batch_el():
    df=get_goodreads_feed(start=gcp.bq_create_dataset())
    gcp.import_df_to_bq(df=df)

@schedule(
    cron_schedule="*/30 * * * *",
    job=batch_el,
    execution_timezone="US/Central",
)
def swa_elt_schedule(context):
    date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return {"ops": {"hello": {"config": {"date": date}}}}

@repository
def swa_elt_repository():
    return [batch_el, swa_elt_schedule]
