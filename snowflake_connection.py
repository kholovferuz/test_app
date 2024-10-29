import snowflake.connector as sf
import os

def connect_snowflake():
    """
    Establish a connection to the Snowflake database.

    This function attempts to connect to the Snowflake database using the specified
    credentials and configurations. If the connection is successful, it returns the 
    connection object. In case of a session expiration error (error code 390114), 
    it attempts to re-authenticate and reconnect.

    Returns:
        sf.connection: A connection object for the Snowflake database if successful, 
                       otherwise raises an exception.

    Raises:
        sf.errors.ProgrammingError: If a programming error occurs while attempting 
                                    to connect to Snowflake.
    """
    try:
        
        conn = sf.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE')
        )
        print("Snowflake connection established.")
        return conn
    except Error as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None
            
# create a cursor object
cur=connect_snowflake().cursor()

# a query to keep the session alive
while True:
    cur = conn.cursor()
    cur.execute("SELECT 1") 
    time.sleep(210 * 60)  
    cur.close()

conn.close()


def create_external_stage(aws_key_id, aws_secret_key):
    """
    Create an external stage in Snowflake for data loading.

    This function creates or replaces an external stage in the specified Snowflake 
    database and schema, allowing for the loading of data from an external source 
    (e.g., AWS S3). The stage is created with the provided AWS credentials and 
    the S3 bucket URL obtained from environment variables.

    Parameters:
        aws_key_id (str): The AWS access key ID for authentication.
        aws_secret_key (str): The AWS secret access key for authentication.

    Raises:
        sf.errors.ProgrammingError: If a programming error occurs during the 
                                    execution of the SQL command.
    """
    cur.execute(f"""
        CREATE OR REPLACE STAGE WEATHER_DB.L0_LANDING_AREA.MY_EXTERNAL_STAGE
        URL=os.getenv('S3_BUCKET')
        CREDENTIALS=(AWS_KEY_ID='{aws_key_id}' AWS_SECRET_KEY='{aws_secret_key}'); 
""")
    print('Stage created')
