"""
Relies on user authentication with:
gcloud auth application-default login
"""
from google.cloud import storage
gcs = storage.Client('​rare-basis-686')
CLOUD_STORAGE_BUCKET = 'im-training'
bucket = storage.bucket.Bucket(gcs, CLOUD_STORAGE_BUCKET)


def get_blob(blob_name):
    return storage.blob.Blob(blob_name, bucket)
