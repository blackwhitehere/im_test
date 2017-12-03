import os
import config
from google.cloud import bigquery

# this sets the env variable for service authentication
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), config.bigquery_auth_file_name)
client = bigquery.Client()


def run_query(q):
    query_job = client.query(q)
    return query_job.result()
