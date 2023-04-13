import os
import pandas as pd

from google.cloud import bigquery as bq
from google.api_core.exceptions import BadRequest
from loguru import logger

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = \
     './tokens/oddjob-db-2007-759fe782b144.json'

client = bq.Client()

def send_stats(account):
    df = pd.read_csv(f"tmp_data/{account}.tsv", delimiter="\t")
    table_ref = client.dataset("yandex_oddjob").table(
        f"dw_report_{account}")
    jobdel = client.delete_table(table_ref)
    logger.info(jobdel.result())
    jobcre = client.create_table(table_ref)
    logger.info(jobcre.result())
    job = client.load_table_from_dataframe(df, table_ref)
    logger.success(job.result())
