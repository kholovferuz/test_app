import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
import snowflake.connector
import requests
cities_list=['Milan','Bologna','Cagliari','Rome','Turin','Naples','Genova']
#
def fetch_current_weather_data(city, api_key, country_code):
    
    api_url=f"https://api.openweathermap.org/data/2.5/weather?q={city},{country_code}&appid={api_key}"
    response=requests.get(api_url)
    if response.status_code==200:
        weather_data=response.json()
    else:
        return "Api not reachable..."
    city_weather = {
            'city': weather_data['name'],
            'temperature': round(weather_data['main']['temp']-273.15,2),  
            'weather_condition': weather_data['weather'][0]['main'],  
            'description': weather_data['weather'][0]['description'],  
            'wind_speed': weather_data['wind']['speed'],  # Wind speed
            'humidity': weather_data['main']['humidity'],
            'high': round(weather_data['main']['temp_max']-273.15,2),
            'low': round(weather_data['main']['temp_min']-273.15,2),
            'icon':weather_data['weather'][0]['icon']
        }
    return city_weather

# asynchronously fetches the weather data for a specific city
async def fetch_city_weather(session, city, api_key, country_code):
    try:
        api_url = f"https://api.openweathermap.org/data/2.5/weather?q={city},{country_code}&appid={api_key}"
        async with session.get(api_url) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"Failed to fetch data for {city}: Status code {response.status}")
                return None
    except Exception as e:
        print(f"Error fetching weather data for {city}: {e}")
        return None

# fetches weather data for a list of cities concurrently by making asynchronous API calls for each city
async def fetch_weather_data_async(cities_list, api_key, country_code):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_city_weather(session, city, api_key, country_code) for city in cities_list]
        weather_data_list = await asyncio.gather(*tasks)
    return [data for data in weather_data_list if data is not None]

async def main(cities_list, api_key, country_code):
    weather_data = await fetch_weather_data_async(cities_list, api_key, country_code)
    return weather_data

def flatten_raw_data(weather_data_response):
    try:
        flattened_data = [
            {
                "city_id": city.get('id'),
                "city_name": city.get('name'),
                "country": city.get('sys', {}).get('country'),
                "longitude": city.get('coord', {}).get('lon'),
                "latitude": city.get('coord', {}).get('lat'),
                "weather_id": city.get('weather', [{}])[0].get('id') if city.get('weather') else None,
                "weather_main": city.get('weather', [{}])[0].get('main') if city.get('weather') else None,
                "weather_description": city.get('weather', [{}])[0].get('description') if city.get('weather') else None,
                "weather_icon": city.get('weather', [{}])[0].get('icon') if city.get('weather') else None,
                "base": city.get('base'),
                "temperature": city['main'].get('temp', 0),
                "temp_feels_like": city['main'].get('feels_like', 0),
                "temp_min": city['main'].get('temp_min', 0),
                "temp_max": city['main'].get('temp_max', 0),
                "pressure": city['main'].get('pressure'),
                "humidity": city['main'].get('humidity'),
                "sea_level": city['main'].get('sea_level'),
                "grnd_level": city['main'].get('grnd_level'),
                "visibility": city.get('visibility'),
                "wind_speed": city['wind'].get('speed'),
                "wind_deg": city['wind'].get('deg'),
                "wind_gust": city['wind'].get('gust', 0),
                "rain": city.get('rain', {}).get('1h', 0),
                "clouds": city.get('clouds', {}).get('all', 0),
                "observation_time": city.get('dt', 0),
                "sys_type": city.get('sys', {}).get('type'),
                "id": city.get('sys', {}).get('id'),
                "sunrise": city.get('sys', {}).get('sunrise'),
                "sunset": city.get('sys', {}).get('sunset'),
                "timezone": city.get('timezone'),
                "observation_id": city.get('id'),
                "observation_cod": city.get('cod')
            }
            for city in weather_data_response
        ]
        return flattened_data
    except Exception as e:
        print(f"Error processing weather data: {e}")
        return []

#
def insert_raw_data(flattened_raw_data, cur):
    df = pd.DataFrame(flattened_raw_data)

    try:
        insert_statement = f"""
        INSERT INTO WEATHER_DB.L0_LANDING_AREA.RAW_DATA (
            city_id, city_name, country, latitude, longitude, 
            weather_id, weather_main, weather_description, weather_icon, 
            base, temperature, temp_feels_like, temp_min, temp_max, 
            pressure, humidity, sea_level, grnd_level, 
            visibility, wind_speed, wind_deg, wind_gust, 
            rain, clouds, observation_time, sys_type, 
            id, sunrise, sunset, timezone, observation_id, observation_cod
        ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # converting DataFrame to list of tuples to assign to the insert statement
        data_to_insert = [tuple(row) for row in df.to_numpy()]
        
        # doing bach insert operation
        cur.executemany(insert_statement, data_to_insert)

        print(f"{cur.rowcount} rows inserted successfully.")
        
    except Exception as e:
        print(f"Error inserting data into Snowflake: {e}")

#
def transform_raw_data(flattened_raw_data):
    try:
        flattened_data = [
            {
                "city_id": city['city_id'],
                "city_name": city['city_name'],
                "country": city['country'],
                "longitude": city['longitude'],
                "latitude": city['latitude'],
                "weather_id": city['weather_id'],
                "weather_condition": city['weather_main'] ,
                "weather_description": city['weather_description'],
                "temperature": round(city['temperature'] - 273.15, 2),  # converting from Kelvin to Celsius
                "temp_feels_like": round(city['temp_feels_like'] - 273.15, 2),
                "temp_min": round(city['temp_min'] - 273.15, 2),
                "temp_max": round(city['temp_max'] - 273.15, 2),
                "humidity": city['humidity'],
                "wind_speed": city['wind_speed'],
                "wind_deg": city['wind_deg'],
                "rain": city['rain'],
                "clouds": city['clouds'],
                "observation_time": datetime.fromtimestamp(city.get('observation_time', 0)).strftime('%Y-%m-%d %H:%M:%S'),
                "observation_id": city['observation_id']
            }
            for city in flattened_raw_data
        ]
        return flattened_data
    except Exception as e:
        print(f"Error processing weather data: {e}")
        return []

#
def insert_city_data(transformed_data, cur):
    # merging the table in order to avoid duplicate rows
    try:
        city_records = [(city['city_id'], city['city_name'], city['country'], city['latitude'], city['longitude']) for city in transformed_data]
        cur.executemany("""
            MERGE INTO WEATHER_DB.L1_TRANSFORMATION.CITIES AS target
            USING (SELECT %s AS city_id, %s AS city_name, %s AS country, %s AS latitude, %s AS longitude) AS source
            ON target.city_id = source.city_id
            WHEN MATCHED THEN 
                UPDATE SET city_name = source.city_name, country = source.country, latitude = source.latitude, longitude = source.longitude
            WHEN NOT MATCHED THEN 
                INSERT (city_id, city_name, country, latitude, longitude) 
                VALUES (source.city_id, source.city_name, source.country, source.latitude, source.longitude);
        """, city_records)
        print("Batch city data inserted.")
    except Exception as e:
        print(f"Error inserting city data: {e}")

#
def insert_weather_data(transformed_data, cur):
    try:
        weather_records = [
            (weather['weather_id'], weather['weather_condition'], weather['weather_description'])
            for weather in transformed_data
        ]
        
        cur.executemany("""
            MERGE INTO WEATHER_DB.L1_TRANSFORMATION.WEATHER AS target
            USING (SELECT %s AS weather_id, %s AS weather_condition, %s AS weather_description) AS source
            ON target.weather_id = source.weather_id
            WHEN MATCHED THEN 
                UPDATE SET weather_condition = source.weather_condition, weather_description = source.weather_description
            WHEN NOT MATCHED THEN 
                INSERT (weather_id, weather_condition, weather_description) 
                VALUES (source.weather_id, source.weather_condition, source.weather_description);
        """, weather_records)
        
        print("Batch weather data merged.")
    except Exception as e:
        print(f"Error merging weather data: {e}")

#
def insert_weather_observations(transformed_data, cur):
    try:
        observation_records = [
            (observation['observation_id'],observation['city_id'], observation['weather_id'], observation['temperature'], observation['temp_feels_like'], 
             observation['temp_min'], observation['temp_max'], observation['humidity'], observation['wind_speed'], 
             observation['wind_deg'], observation['rain'], observation['clouds'], observation['observation_time'])
            for observation in transformed_data
        ]
        cur.executemany("""
            INSERT INTO WEATHER_DB.L1_TRANSFORMATION.WEATHER_OBSERVATIONS 
            (observation_id, city_id, weather_id, temperature, temp_feels_like, temp_min, temp_max, humidity, wind_speed, wind_deg, rain, clouds, observation_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TO_TIMESTAMP(%s, 'YYYY-MM-DD HH24:MI:SS'));
        """, observation_records)
        print("Batch weather observations inserted.")
    except Exception as e:
        print(f"Error inserting weather observations: {e}")

#
def create_denormalized_table(cur):
    try:
        cur.execute("""
            CREATE OR REPLACE TABLE WEATHER_DB.L2_ANALYSIS.WEATHER_ANALYSIS AS
            SELECT 
                wo.observation_id, wo.observation_time, c.city_id, c.city_name, c.country, c.latitude, c.longitude,
                w.weather_id, w.weather_condition, w.weather_description, wo.temperature, wo.temp_feels_like, 
                wo.temp_min, wo.temp_max, wo.humidity, wo.wind_speed, wo.wind_deg, wo.rain, wo.clouds
            FROM WEATHER_DB.L1_TRANSFORMATION.WEATHER_OBSERVATIONS wo
            JOIN WEATHER_DB.L1_TRANSFORMATION.CITIES c ON wo.city_id = c.city_id
            JOIN WEATHER_DB.L1_TRANSFORMATION.WEATHER w ON wo.weather_id = w.weather_id;
        """)
        print("Denormalized table created successfully.")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Programming error while creating denormalized table: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

#
def fetch_denormalized_into_df(st, cur):
    try:
        cur.execute("SELECT * FROM WEATHER_DB.L2_ANALYSIS.WEATHER_ANALYSIS;")

        # fetching the result to pandas dataframe immediately
        df = cur.fetch_pandas_all()
        
        print("Data fetched successfully into Pandas DataFrame.")
        
        return df
    except snowflake.connector.errors.ProgrammingError as e:
        st.error(f"Error fetching data from Snowflake: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An error occurred: {e}")

#
def download_data(df):
    df.to_csv('weather_analysis.csv', index=False)

#
def save_denormalized_data_into_bucket(cur):
    today = datetime.now()
    
    previous_day = today - timedelta(days=1)

    formatted_previous_day = previous_day.strftime('%Y-%m-%d')  
    formatted_today=today.strftime('%Y-%m-%d')
    
    cur.execute(f"""
        COPY INTO @L0_LANDING_AREA.my_external_stage/analysis_data/ad_{formatted_today}/
        FROM (
            SELECT * 
            FROM weather_db.L2_ANALYSIS.weather_analysis
            WHERE DATE(OBSERVATION_TIME) = '{today}'
        )
        FILE_FORMAT = (TYPE = 'PARQUET')
        OVERWRITE = TRUE;
        """)
    print("Analysis data successfully saved into bucket")

#
def save_raw_data_into_bucket(cur):
    today = datetime.now()

    previous_day = today - timedelta(days=1)

    formatted_today=today.strftime('%Y-%m-%d')  
    formatted_previous_day = previous_day.strftime('%Y-%m-%d')  

    cur.execute(f"""
        COPY INTO @L0_LANDING_AREA.my_external_stage/raw_data/rd_{formatted_today}/
        FROM (
            SELECT * 
            FROM WEATHER_DB.L0_LANDING_AREA.RAW_DATA
            WHERE DATE(OBSERVATION_TIME) = '{today}'
        )
        FILE_FORMAT = (TYPE = 'PARQUET')
        OVERWRITE = TRUE;
        """)
    print("Raw data successfully saved into a bucket")

