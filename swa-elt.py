from dagster import job, op, repository, schedule, ScheduleDefinition
from dagster.utils.log import get_dagster_logger
from dagster_gcp.gcs.io_manager import gcs_pickle_io_manager
from dagster_gcp.gcs.resources import gcs_resource
from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
import pandas as pd

logger = get_dagster_logger()

@op
def generate_df_from_goodreads_soup():
    """Extract a dataframe of the past 100 items in my public Goodreads RSS feed"""
    page = "https://www.goodreads.com/review/list_rss/51384411?key=WtDvvUkHkxtfdlRlZRZ5v3AFe4OOq3SGxiQ0xn4NV5oDUDvJ&shelf=%23ALL%23"
    html = urlopen(page)
    soup = BeautifulSoup(html, "lxml")
    filtered_soup = soup.find_all("description")
    title_data = []
    detail_data = []
    # first row blank, 100 item feed
    for item in range(1, 101):
        row = filtered_soup[item]

        title_tag = row.find(re.compile("img"))
        title_text = title_tag.get("alt")
        title_data.append(title_text)

        row_data = row.text.strip()
        row_data = row_data.split("\n")
        row_data = [x.split(":", 1)[-1] for x in row_data]
        row_data = [x.strip() for x in row_data]
        detail_data.append(row_data)

    df_title = pd.DataFrame(columns=["title"], data=title_data)
    df_detail = pd.DataFrame(
        columns=[
            "author",
            "name",
            "average_rating",
            "book_published",
            "rating",
            "read_at",
            "date_added",
            "shelves",
            "review",
            "idk",
            "idk2",
            "idk3"
        ],
        data=detail_data,
    )
    df = pd.concat([df_title, df_detail], axis=1)
    # drop final column since it is computer junk
    df = df.iloc[:, :-1]
    df = df.set_index('title')
    logger.info(df.head())
    return df


@job(
    resource_defs={
        "gcs": gcs_resource,
        "io_manager": gcs_pickle_io_manager,
    },
    config={
        "resources": {
            "io_manager": {
                "config": {
                    "gcs_bucket": "small-world-dagster",
                    "gcs_prefix": "dagster-logs",
                }
            }
        }
    },
)
def batch_el():
    generate_df_from_goodreads_soup()

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
