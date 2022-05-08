"""
Verifies the core functionality of sqlite tasks.
"""

from prefect import Flow, task
from prefect.client import Secret
from prefect.tasks.databricks import (
    DatabricksGetJobID,
    DatabricksRunNow,
    DatabricksSubmitMultitaskRun,
    DatabricksSubmitRun,
)
from prefect.tasks.databricks.models import (
    AutoScale,
    AwsAttributes,
    AwsAvailability,
    JobTaskSettings,
    NewCluster,
    NotebookTask,
)

DATABRICKS_CONN_SECRET = Secret("DATABRICKS_CONNECTION_STRING").get()
NOTEBOOK_PATH = "/Users/andrew.h@prefect.io/records"
BASE_PARAMETERS = {
    "station": "ORD",
    "network": "IL_ASOS",
    "stat": "max",
    "column": "max_temp_f",
}


@task
def _show_output(std_out):
    """
    Displays the task's results.
    """
    print(std_out)


with Flow("databricks") as flow:
    databricks_get_job_id = DatabricksGetJobID(
        databricks_conn_secret=DATABRICKS_CONN_SECRET, search_limit=15
    )("testing")
    _show_output(databricks_get_job_id)

    json = {
        "new_cluster": {
            "spark_version": "10.4.x-scala2.12",
            "num_workers": 2,
            "node_type_id": "m4.large",
            "aws_attributes": {
                "ebs_volume_type": "GENERAL_PURPOSE_SSD",
                "ebs_volume_count": 3,
                "ebs_volume_size": 100,
            },
        },
        "notebook_task": {
            "notebook_path": NOTEBOOK_PATH,
            "base_parameters": BASE_PARAMETERS,
        },
    }
    databricks_submit_run = DatabricksSubmitRun(json=json)(
        databricks_conn_secret=DATABRICKS_CONN_SECRET
    )
    _show_output(databricks_submit_run)

    databricks_submit_run = DatabricksRunNow(
        job_id="7179304998089", notebook_params=BASE_PARAMETERS
    )(databricks_conn_secret=DATABRICKS_CONN_SECRET)
    _show_output(databricks_submit_run)

    notebook_task = NotebookTask(
        notebook_path=NOTEBOOK_PATH,
        base_parameters=BASE_PARAMETERS,
    )
    databricks_task = JobTaskSettings(
        task_key="Records",
        description="Finds the historic records of a given station",
        new_cluster=NewCluster(
            spark_version="10.4.x-scala2.12",
            node_type_id="m4.large",
            spark_conf={"spark.speculation": True},
            aws_attributes=AwsAttributes(
                availability=AwsAvailability.SPOT,
                zone_id="us-west-2a",
                ebs_volume_type="GENERAL_PURPOSE_SSD",
                ebs_volume_count=3,
                ebs_volume_size=100,
            ),
            autoscale=AutoScale(min_workers=1, max_workers=2),
        ),
        notebook_task=notebook_task,
        databricks_retry_limit=5,
        databricks_retry_delay=60,
        timeout_seconds=86400,
    )
    submit_multitask_run = DatabricksSubmitMultitaskRun(
        databricks_conn_secret=DATABRICKS_CONN_SECRET
    )(tasks=[databricks_task])
    _show_output(submit_multitask_run)


if __name__ == "__main__":
    flow.run()
