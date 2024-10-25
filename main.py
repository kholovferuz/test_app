import asyncio
from snowflake_connection import cur
import os
from data_orchestration.dags.modules.api_calls import ( 
    main,
    flatten_raw_data,
    transform_raw_data,
    insert_raw_data,
    insert_city_data,
    insert_weather_data,
    insert_weather_observations,
    create_denormalized_table,
    cities_list
)

if __name__=="__main__":
    weather_data_unflattened=asyncio.run(main(cities_list,os.getenv('API_KEY'), os.getenv('COUNTRY_CODE')))
    weather_raw_data_flattened=flatten_raw_data(weather_data_unflattened)
    # weather_transformed_data_flattened=transform_raw_data(weather_raw_data_flattened)
    # insert_raw_data(weather_raw_data_flattened,cur)
    # insert_city_data(weather_transformed_data_flattened,cur)
    # insert_weather_data(weather_transformed_data_flattened,cur)
    # insert_weather_observations(weather_transformed_data_flattened,cur)
    # create_denormalized_table(cur)

 