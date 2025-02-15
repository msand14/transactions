import time
import requests
from typing import List, Dict
import json
import os
from pyspark.sql import SparkSession
import sys
import shutil


def get_spark():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark = SparkSession.builder.master("local").appName("PySpark Installation Test").getOrCreate()
    return spark


def get_nse_id(claim_id: str, api_url_claim_id: str) -> str:
    response = requests.get(
        f'https://api.hashify.net/hash/md4/hex?value={claim_id}'
    )
    retry_times = 3
    sleep_seconds = 3
    status_code = response.status_code
    if status_code == 200:
        return response.json()['Digest']
    while retry_times:
        response = requests.get(
            f'https://api.hashify.net/hash/md4/hex?value={claim_id}'
        )
        status_code = response.status_code
        if status_code == 200:
            return response.json()['Digest']
        time.sleep(sleep_seconds)
        retry_times -= 1
        # Either we retry a 5XX code status 3 times and
        # still not working or status code is 4XX type
    raise Exception(
        f"'Claim_id' {claim_id} could not be retrieved from API." +
        "Status code: {response.status_code}.Response: {response.text}")


def get_task_parameters() -> List[Dict]:
    with open('configuration/transactions_task_parameters.json', 'r') as file:
        task_parameters = json.load(file)
    return task_parameters


def write_parquet(df, path: str):
    df.write.parquet(path)


def read_parquet(spark, path: str):
    return spark.read.parquet(path)


def read_csv(spark, path, use_header_record: bool = True):
    return spark.read.option("header", use_header_record).csv(path)


def clean_output_directory(path):
    if os.path.exists(path):
        shutil.rmtree(path)
