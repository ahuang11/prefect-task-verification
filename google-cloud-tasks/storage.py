"""
Verifies the core functionality of Google Cloud Storage.
"""

from prefect import Flow, task
from prefect.engine.signals import FAIL
from prefect.tasks.gcp.storage import GCSUpload, GCSDownload, GCSCopy, GCSBlobExists


# replace these values with desired
BUCKET = "prefect-task"
BUCKET_2 = "prefect-task-copy"
PROJECT = "prefect-project"


@task
def show_output(std_out):
    try:
        print(std_out)
        return True
    except:
        return False


with Flow("list_files") as flow:
    blob = "test.txt"
    blob_2 = "test_copy.txt"

    # first upload
    gcs_upload = GCSUpload(
        bucket=BUCKET,
        project=PROJECT,
    )(blob=blob, data="verified!", create_bucket=True)
    show_output(gcs_upload)  # test.txt

    # download it
    gcs_download = GCSDownload(
        bucket=BUCKET,
        project=PROJECT
    )(blob=blob)
    show_output(gcs_download)  # b"verified!"

    # copy it to another bucket
    gcs_copy = GCSCopy(
        source_bucket=BUCKET,
        dest_bucket=BUCKET_2,
        project=PROJECT
    )(source_blob=blob, dest_blob=blob_2, create_bucket=True)
    show_output(gcs_copy)  # test_copy.txt

    # ensure blob_2 exists in BUCKET_2
    gcs_blob_exists = GCSBlobExists(bucket_name=BUCKET_2)
    show_output(gcs_blob_exists(blob=blob_2))  # True


if __name__ == "__main__":
    flow.run()
