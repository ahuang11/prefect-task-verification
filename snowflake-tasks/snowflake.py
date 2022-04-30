"""
Verifies the core functionality of Snowflake tasks.
"""

from prefect import Flow, task
from prefect.client import Secret
from prefect.tasks.snowflake import SnowflakeQueriesFromFile, SnowflakeQuery

QUERY_FILE = "query.sql"
POSTAL_CODE = "10128"
WAREHOUSE = "COMPUTE_WH"
DATABASE = "WEATHERSOURCE_TILE_SAMPLE_SNOWFLAKE_SECURE_SHARE_1641488329256"


@task
def _show_output(std_out):
    """
    Displays the task's results.
    """
    print(std_out)


credentials_kwargs = dict(
    account=Secret("SNOWFLAKE_ACCOUNT").get(),
    user=Secret("SNOWFLAKE_USER").get(),
    password=Secret("SNOWFLAKE_PASSWORD").get(),
    warehouse=WAREHOUSE,
    database=DATABASE,
)

with Flow("snowflake") as flow:
    # read query directly from file
    snowflake_queries_from_file = SnowflakeQueriesFromFile(**credentials_kwargs)(
        file_path=QUERY_FILE
    )
    _show_output(snowflake_queries_from_file)

    # load query and execute
    with open(QUERY_FILE, "r") as f:
        # test params
        query = f.read().replace(f"'{POSTAL_CODE}'", "%s")
    snowflake_query = SnowflakeQuery(**credentials_kwargs)(
        query=query, data=(POSTAL_CODE,)
    )
    _show_output(snowflake_query)

flow.run()
