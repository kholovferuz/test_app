"""
Airflow DAG for Weather Data Pipeline

This script defines an Apache Airflow Directed Acyclic Graph (DAG) for a weather data 
pipeline. The pipeline retrieves weather data from an API, processes it, and stores 
the results in a Snowflake database, along with saving raw and denormalized data 
to an S3 bucket.

The DAG consists of the following main tasks:
1. Fetch weather data from the API.
2. Flatten the raw data retrieved.
3. Insert the raw data into Snowflake.
4. Transform the flattened data for further analysis.
5. Insert city and weather data into Snowflake.
6. Create a denormalized table in Snowflake.
7. Save raw and denormalized data into an S3 bucket.

Dependencies are managed using XCom to pass data between tasks, and the 
workflow is scheduled to trigger at 10 minutes past every hour.
"""
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from modules.api_calls import (main, flatten_raw_data, transform_raw_data, insert_raw_data, insert_city_data, insert_weather_data,cities_list,
                               insert_weather_observations, create_denormalized_table, save_denormalized_data_into_bucket, save_raw_data_into_bucket )
from airflow.hooks.base import BaseHook
import snowflake.connector
import asyncio
from datetime import datetime, timedelta
import os

def get_snowflake_connection():
    """
    Establish a connection to the Snowflake database.

    This function retrieves the Snowflake connection details 
    from Airflow's connection manager and returns a Snowflake 
    connection object.

    Returns:
        snowflake.connector.connection: A connection object for the Snowflake database.
    """

    # Retrieve the connection
    connection = BaseHook.get_connection('snowflake_conn')
    
    # Check if necessary environment variables are available
    snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT')
    snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
    snowflake_database = os.getenv('SNOWFLAKE_DATABASE')

    # Debug logging
    print(f"Connecting to Snowflake with account: {connection.host}, user: {connection.login}")

    # Error handling for missing credentials
    if not connection.host or not connection.login or not connection.password:
        raise ValueError("Snowflake connection parameters are not fully defined.")
    if not snowflake_account or not snowflake_warehouse or not snowflake_database:
        raise ValueError("One or more required environment variables (SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE) are not set.")

    # Establish connection
    return snowflake.connector.connect(
        user=connection.login,
        password=connection.password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database
    )

# defining default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 2,
}

# defining the DAG
with DAG('weather_data_pipeline',
         default_args=default_args,
         start_date=datetime(2024, 10, 23),
         tags=['elt, data transformation'],
         schedule_interval='10 * * * *',
         catchup=False) as dag:
    
    def fetch_data(cities_list, api_key, country_code):
        """
        Fetch weather data from the API.

        This function uses the provided API key, city list, and 
        country code to retrieve weather data asynchronously.

        Parameters:
            cities_list (list): A list of cities to fetch weather data for.
            api_key (str): The API key for authentication.
            country_code (str): The country code for the cities.

        Returns:
            dict: The weather data retrieved from the API.
        
        Raises:
            ValueError: If the API key is missing.
        """
        if not api_key:
            raise ValueError("API Key is missing!")
        print(f"Using API Key: {api_key}")
        weather_data = asyncio.run(main(cities_list, api_key, country_code))
        return weather_data
    
    def flatten_r_data(ti):
        """
        Flatten the raw weather data.

        This function retrieves the raw weather data from XCom 
        and flattens it for easier processing.

        Parameters:
            ti (TaskInstance): The current task instance for accessing XCom.

        Returns:
            dict: The flattened weather data.
        """

        raw_data = ti.xcom_pull(task_ids='fetch_weather')
        flattened_data = flatten_raw_data(raw_data)
        return flattened_data
    
    def insert_r_data(ti):
        """
        Insert flattened raw data into Snowflake.

        This function retrieves the flattened data from XCom 
        and inserts it into the Snowflake database.

        Parameters:
            ti (TaskInstance): The current task instance for accessing XCom.
        """

        flattened_data = ti.xcom_pull(task_ids='flatten_raw_data')
        conn = get_snowflake_connection()
        with conn.cursor() as cur:
            insert_raw_data(flattened_data, cur)

    def transform_r_data(ti):
        """
        Transform the flattened raw data.

        This function retrieves the flattened data from XCom and 
        transforms it for analysis.

        Parameters:
            ti (TaskInstance): The current task instance for accessing XCom.

        Returns:
            dict: The transformed weather data.
        """

        flattened_raw_data=ti.xcom_pull(task_ids='flatten_raw_data')
        return transform_raw_data(flattened_raw_data)
    
    def insert_city_data_d(ti):
        """
        Insert transformed city data into Snowflake.

        This function retrieves the transformed city data from 
        XCom and inserts it into the Snowflake database.

        Parameters:
            ti (TaskInstance): The current task instance for accessing XCom.
        
        Raises:
            ValueError: If no data is retrieved from the transform_raw_data task.
        """
        transformed_data = ti.xcom_pull(task_ids='transform_raw_data')
        print(f"Transformed city data: {transformed_data}")
        
        if not transformed_data:
            raise ValueError("No data was retrieved from transform_raw_data task.")
        
        conn = get_snowflake_connection()
        with conn.cursor() as cur:
            insert_city_data(transformed_data, cur)
        print("City data insertion completed.")

    def insert_weather_data_d(ti):
        """
        Insert transformed weather data into Snowflake.

        This function retrieves the transformed weather data from 
        XCom and inserts it into the Snowflake database.

        Parameters:
            ti (TaskInstance): The current task instance for accessing XCom.
        """

        transformed_data=ti.xcom_pull(task_ids='transform_raw_data')
        conn = get_snowflake_connection()
        with conn.cursor() as cur:
            insert_weather_data(transformed_data,cur)

    def insert_weather_observations_d(ti):
        """
        Insert transformed weather observations into Snowflake.

        This function retrieves the transformed weather observations 
        from XCom and inserts them into the Snowflake database.

        Parameters:
            ti (TaskInstance): The current task instance for accessing XCom.
        """

        transformed_data=ti.xcom_pull(task_ids='transform_raw_data')
        conn = get_snowflake_connection()
        with conn.cursor() as cur:
            insert_weather_observations(transformed_data,cur)

    def create_denormalized_table_t():
        """
        Create a denormalized table in Snowflake.

        This function creates a denormalized table in Snowflake 
        for optimized querying and analysis of weather data.
        """

        conn = get_snowflake_connection()
        with conn.cursor() as cur:
            create_denormalized_table(cur)

    def get_next_midnight():
        """
        Get the next midnight time.

        This function calculates the datetime of the next midnight 
        for use in scheduling tasks.

        Returns:
            datetime: The next midnight datetime.
        """
        
        now = datetime.utcnow()
        next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return next_midnight
    
    # tasks
    fetch_data_task = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_data,
        op_kwargs={"cities_list": cities_list, "api_key": os.getenv('API_KEY'), "country_code": os.getenv('COUNTRY_CODE')}
    )

    flatten_raw_data_task = PythonOperator(
        task_id='flatten_raw_data',
        python_callable=flatten_r_data,
    )

    insert_raw_data_task = PythonOperator(
        task_id='insert_raw_data',
        python_callable=insert_r_data
    )

    transform_raw_data_task=PythonOperator(
        task_id='transform_raw_data',
        python_callable=transform_r_data
    )

    insert_city_data_task=PythonOperator(
        task_id='insert_city_data',
        python_callable=insert_city_data_d
    )

    insert_weather_data_task=PythonOperator(
        task_id='insert_weather_data',
        python_callable=insert_weather_data_d
    )

    insert_weather_observations_task=PythonOperator(
        task_id='insert_weather_observations',
        python_callable=insert_weather_observations_d
    )

    create_denormalized_table_task=PythonOperator(
        task_id='create_denormalized_table',
        python_callable=create_denormalized_table_t
    )

    
    # task dependencies
    chain(fetch_data_task, flatten_raw_data_task, [insert_raw_data_task, transform_raw_data_task])

    transform_raw_data_task >> [insert_city_data_task, insert_weather_data_task, insert_weather_observations_task]
    
    [insert_city_data_task, insert_weather_data_task, insert_weather_observations_task] >> create_denormalized_table_task

    