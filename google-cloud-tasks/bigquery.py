"""
Verifies the core functionality of Google Cloud Platform BigQuery.
"""

from pathlib import Path

from google.cloud.bigquery import SchemaField
from prefect import Flow, task
from prefect.tasks.gcp import (
    BigQueryLoadFile,
    BigQueryLoadGoogleCloudStorage,
    BigQueryStreamingInsert,
    BigQueryTask,
    CreateBigQueryTable,
)

PROJECT = "prefect-dev-807fd2"
DATASET = "prefect_dataset"  # no hyphens


@task
def _show_output(std_out):
    """
    Displays the task's results.
    """
    print(std_out)


with Flow("gcp_bigquery") as flow:
    # create a new table, and dataset, if they don't exist
    table = "prefect_table"
    create_bigquery_table = CreateBigQueryTable(project=PROJECT, dataset=DATASET)(
        table=table
    )
    _show_output(create_bigquery_table)

    # load a file into bigquery
    schema = [
        SchemaField("string_col", "STRING", "REQUIRED"),
        SchemaField("int_col", "INTEGER", "REQUIRED"),
        SchemaField("bool_col", "BOOL", "NULLABLE"),
    ]
    file = Path("data") / "prefect_data.csv"
    bigquery_load_file = BigQueryLoadFile(project=PROJECT, dataset_id=DATASET)(
        table=table, file=file, schema=schema
    )
    _show_output(bigquery_load_file)

    # insert two rows
    records = [
        {"string_col": "c", "int_col": 3, "bool_col": False},
        {"string_col": "d", "int_col": 4, "bool_col": False},
    ]
    bigquery_streaming_insert = BigQueryStreamingInsert(
        project=PROJECT, dataset_id=DATASET
    )(table=table, records=records)
    _show_output(bigquery_streaming_insert)

    # query table to see whether inserted
    query = f"SELECT * FROM {DATASET}.{table} WHERE bool_col=@bool_value"
    query_params = [("bool_value", "BOOL", "false")]
    bigquery_task = BigQueryTask(project=PROJECT)(
        query=query, query_params=query_params
    )
    _show_output(bigquery_task)

    # create table by pulling data from GCS bucket
    # NOTE: this will fail without modifying uri!
    uri = "gs://integrations-dev/test.csv"
    new_table = "prefect_table_2"
    bigquery_load_google_cloud_storage = BigQueryLoadGoogleCloudStorage(
        uri=uri, dataset_id=DATASET, table=new_table
    )
    _show_output(bigquery_load_google_cloud_storage)


if __name__ == "__main__":
    flow.run()
