"""
Verifies the core functionality of Google Cloud Platform Secret Manager.
"""

from prefect import Flow, task
from prefect.tasks.transform import TransformCreateMaterialization

# replace these values with desired
API_KEY = "tfdk-..."
MQL_SERVER_URL = "https://tfd-external.prod.transformdata.io/"


@task
def _show_output(std_out):
    """
    Displays the task's results.
    """
    print(std_out)


with Flow("transform_data") as flow:
    # to see materializations, run `mql list-materializations`
    materialization_name = "materialization_name"

    transform_create_materialization = TransformCreateMaterialization(
        api_key=API_KEY,
        mql_server_url=MQL_SERVER_URL,
    )(materialization_name=materialization_name)
    _show_output(transform_create_materialization)


if __name__ == "__main__":
    flow.run()
