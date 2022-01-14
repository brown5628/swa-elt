from dagster import job, op, repository, schedule, ScheduleDefinition
from dagster.utils.log import get_dagster_logger
#from dagster_gcp.gcs.io_manager import gcs_pickle_io_manager
#from dagster_gcp.gcs.resources import gcs_resource

logger = get_dagster_logger()

@op
def get_name():
    return "dagster"

@op
def get_date():
    return "today"

@op
def hello(name:str, date:str):
    logger.info("It worked! Test!")
    print(f"Hello, {name}! This is dagster on {date}.")


#@job(
#    resource_defs={
#        "gcs": gcs_resource,
#        "io_manager": gcs_pickle_io_manager,
#    },
#    config={
#        "resources": {
#            "io_manager": {
#                "config": {
#                    "gcs_bucket": "small-world-dagster",
#                    "gcs_prefix": "dagster-logs-",
#                }
#            }
#        }
#    },
#)
@job
def hello_dagster():
    hello(get_name(),get_date())

@schedule(
    cron_schedule="*/30 * * * *",
    job=hello_dagster,
    execution_timezone="US/Central",
)
def test_schedule(context):
    date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return {"ops": {"hello": {"config": {"date": date}}}}

@repository
def swa_elt_repository():
    return [hello_dagster, test_schedule]