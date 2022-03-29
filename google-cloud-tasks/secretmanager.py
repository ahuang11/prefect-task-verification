"""
Verifies the core functionality of Google Cloud Platform Secret Manager.
"""

from prefect import Flow, task
from prefect.tasks.gcp.secretmanager import GCPSecret

# replace these values with desired
PROJECT = "prefect-project"


@task
def _show_output(std_out):
    """
    Displays the task's results.
    """
    print(std_out)


with Flow("gcp_secretmanager") as flow:
    secret_id = "a-prefect-secret"
    gcp_secret = GCPSecret(PROJECT)(secret_id=secret_id, version_id=1)
    _show_output(gcp_secret)


if __name__ == "__main__":
    flow.run()
