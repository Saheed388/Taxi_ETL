from google.cloud import bigquery
from google.cloud import storage
import os
import pandas as pd
import numpy as np
import tempfile

"""
This Script moves NYC Taxi Trips data from GCS Bucket to BigQuery Table
"""

BUCKET = os.getenv("GCP_GCS_BUCKET", "uber_data_n1")

# Construct a BigQuery client object
client = bigquery.Client()
dataset_name = "uberdata_1"
services = "fhv"
project_id = "alt-school-project-386517"
table_id = f"{project_id}.{dataset_name}.{services}"

# Define table schema
schema = [
    bigquery.SchemaField("dispatching_base_num", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("pickup_datetime", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("dropOff_datetime", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("PUlocationID", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("DOlocationID", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("SR_Flag", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("Affiliated_base_number", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("season", "STRING", mode="NULLABLE"),  # Add the new field
]


def create_bq_table():
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")


def gcs_to_bigquery():
    bucket_name = BUCKET
    foldername = "fhv/2019"

    # Retrieve all blobs with a prefix matching the folder.
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # List blobs in the folder
    blobs = bucket.list_blobs(prefix=foldername)

    print(f"About to download files from {bucket_name}")

    for blob in blobs:
        print(blob.name)

        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmpfile:
            destination_uri = tmpfile.name
            print(destination_uri)
            blob.download_to_filename(destination_uri)

            df = pd.read_parquet(destination_uri)
            print(df.head())
            print(df.columns.values)

            # Data cleaning
            df = df.rename(
                columns={
                    "dispatching_base_num": "dispatching_base_num",
                    "PUlocationID": "PUlocationID",
                    "DOlocationID": "DOlocationID",
                    "SR_Flag": "SR_Flag",
                    "Affiliated_base_number": "Affiliated_base_number",
                }
            )

            # Remove duplicates
            # df = df.drop_duplicates()
            # df = df.drop(["trip_type"], axis=1)
            # df = df.drop(["ehail_fee"], axis=1)

            # Replace missing values in rate_code_id with the most common rate code ID
            # df["rate_code_id"] = df["rate_code_id"].fillna(df["rate_code_id"].mode()[0])

            # Drop rows with zero values in certain columns
            # df = df[df["distance"] > 0]
            # df = df[df["fare"] > 0]
            df.dropna(inplace=True)

            # Convert pickup_datetime and dropoff_datetime to pandas datetime
            df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
            df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])

            # Categorize trips into seasons
            conditions = [
                (df["pickup_datetime"].dt.month.isin([12, 1, 2])),
                (df["pickup_datetime"].dt.month.isin([3, 4, 5])),
                (df["pickup_datetime"].dt.month.isin([6, 7, 8])),
                (df["pickup_datetime"].dt.month.isin([9, 10, 11])),
            ]
            values = ["Winter", "Spring","attum", "Summer"]
            df["season"] = np.select(conditions, values, default="Unknown")

            # Convert pandas dataframe to BigQuery table
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition="WRITE_APPEND",
            )

            job = client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )  # Make an API request.
            job.result()  # Wait for the job to complete.

            print(f"Loaded {job.output_rows} rows into {table_id}")

    print("Data successfully loaded into BigQuery.")


create_bq_table()
gcs_to_bigquery()
