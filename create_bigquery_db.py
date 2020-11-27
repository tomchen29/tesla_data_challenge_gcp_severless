
from google.cloud import bigquery
from google.api_core.exceptions import Conflict


def create_bigquery_table():

    project_name = 'tesla-iot-challenge'
    dataset_name = 'telemetry'
    table_name = 'energy_telemetry_logging'
    # -----------------
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # -----------------
    # Create Dataset
    try:
        client.create_dataset(bigquery.Dataset(
            f'{project_name}.{dataset_name}'))
    except Conflict:
        if Conflict.code == 409:
            print('dataset already created. Skip this process')
        else:
            raise Conflict

    # -----------------
    # Create Table

    # set table schema
    schema = [
        bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("site", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("SITE_SM_batteryInstPower",
                             "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("SITE_SM_siteInstPower",
                             "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("SITE_SM_solarInstPower",
                             "FLOAT", mode="NULLABLE"),
    ]
    # create table instance
    table_id = f"{project_name}.{dataset_name}.{table_name}"
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.HOUR,
        field="event_timestamp",  # name of column to use for partitioning
        expiration_ms=7776000000,  # 90 days
    )
    try:
        # Request to create table
        table = client.create_table(table)
    except Conflict:
        if Conflict.code == 409:
            print('table already created. Skip this process')
        else:
            raise Conflict
