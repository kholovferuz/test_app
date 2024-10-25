from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models.baseoperator import chain
from modules.api_calls import (main, flatten_raw_data, transform_raw_data, insert_raw_data, insert_city_data, insert_weather_data,
                               insert_weather_observations, create_denormalized_table, save_denormalized_data_into_bucket, save_raw_data_into_bucket )
from airflow.hooks.base import BaseHook
import snowflake.connector
import asyncio, os
from datetime import datetime, timedelta

def get_snowflake_connection():
    """
    Establish a connection to the Snowflake database.

    This function retrieves the Snowflake connection details 
    from Airflow's connection manager and returns a Snowflake 
    connection object.

    Returns:
        snowflake.connector.connection: A connection object for the Snowflake database.
    """

    connection = BaseHook.get_connection('snowflake_conn')
     # Debug logging
    print(f"Connecting to Snowflake with account: {connection.host}, user: {connection.login}")
    
    if connection.host is None:
        print("Snowflake account (host) is not defined in the connection.")
    
    return snowflake.connector.connect(
        user=connection.login,
        password=connection.password,
        account=os.getenv('SNOWFLAKE_ACCOUNT'), 
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE')

    )

# defining default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 2,
}

# defining the DAG
with DAG('save_into_bucket_dag',
         default_args=default_args,
         start_date=datetime(2024, 10, 23),
         tags=['elt, data transformation'],
         schedule_interval='30 0 * * *',
         catchup=False) as dag:
    

    def save_r_data_into_bucket():
        """
        Save raw data into S3 bucket.
        This function saves the raw weather data from Snowflake 
        into an S3 bucket for backup and further analysis.
        """
        conn = get_snowflake_connection()
        with conn.cursor() as cur:
            save_raw_data_into_bucket(cur)

    def save_denorm_table_into_bucket():
        """
        Save the denormalized table into S3 bucket.

        This function saves the denormalized weather data 
        from Snowflake into an S3 bucket.
        """

        conn = get_snowflake_connection()
        with conn.cursor() as cur:
            save_denormalized_data_into_bucket(cur)


    save_raw_data_into_bucket_task = PythonOperator(
        task_id='save_raw_data_into_bucket',
        python_callable=save_r_data_into_bucket
    )

    save_denormalized_data_into_bucket_task=PythonOperator(
        task_id='save_denormalized_data_into_bucket',
        python_callable=save_denorm_table_into_bucket
    )

    # this dag has not been completely yet. I will do it as quick as possible.