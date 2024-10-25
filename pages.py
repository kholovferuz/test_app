import streamlit as st
from data_orchestration.dags.modules.api_calls import fetch_current_weather_data, fetch_denormalized_into_df
from weather_analysis import WeatherAnalysis
import pandas as pd
from datetime import datetime, timedelta

def snowflake_tables(cur):
    """
    Fetches and displays data from Snowflake tables related to weather.

    This function executes SQL queries to retrieve data from the following 
    Snowflake tables: cities, weather, weather observations, and weather 
    analysis. It then calls the `display_table` function to present this 
    data in a user-friendly format within the Streamlit app.

    Parameters:
    - cur: A Snowflake cursor object used to execute SQL queries.
    """

    def display_table(title, df, custom_columns=None, file_prefix="data"):
        """
        Displays a DataFrame as a table in Streamlit and provides an option 
        to download the data as a CSV file.

        Parameters:
        - title (str): The title of the table to display.
        - df (pd.DataFrame): The DataFrame to display.
        - custom_columns (list, optional): List of columns to display. 
                                            If None, all columns are shown.
        - file_prefix (str, optional): Prefix for the downloaded CSV file name.
        """
        if df.empty:
            st.error(f"No data available to display for {title}.")
        else:
            st.write(f"**{title} table** ({len(df)} rows)")
            
            # If custom columns are provided, display only those
            if custom_columns:
                df = df[custom_columns]
            
            st.dataframe(df, hide_index=True)
            
            # CSV download functionality
            csv_content = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label=f"Download {title} data (.csv)",
                data=csv_content,
                file_name=f"{file_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
            )

    # Main Streamlit app
    st.header("The following tables are in the Snowflake database")

    try:
        # Fetch cities table and customize columns
        cities = cur.execute("select * from WEATHER_DB.L1_TRANSFORMATION.CITIES").fetch_pandas_all()
        display_table(
            title="Cities",
            df=cities,
            file_prefix="cities_data"
        )
        
        # Fetch weather table and customize columns
        weather = cur.execute("select * from WEATHER_DB.L1_TRANSFORMATION.WEATHER").fetch_pandas_all()
        display_table(
            title="Weather",
            df=weather,
            file_prefix="weather_data"
        )
        
        # Fetch weather observations table and customize columns
        weather_observations = cur.execute("select * from WEATHER_DB.L1_TRANSFORMATION.WEATHER_OBSERVATIONS").fetch_pandas_all()
        display_table(
            title="Weather Observations",
            df=weather_observations,
            file_prefix="weather_observations"
        )
        
        # Fetch weather analysis table and customize columns
        weather_analysis_table = cur.execute("select * from WEATHER_DB.l2_analysis.weather_analysis").fetch_pandas_all()
        display_table(
            title="Weather Analysis",
            df=weather_analysis_table,
            file_prefix="weather_analysis"
        )

    except Exception as e:
        st.error(f"Error fetching tables: {str(e)}")

def current_weather(cities, api_key, country_code):
        """
        Displays the current weather information for a selected city.

        This function provides a sidebar for city selection and fetches current 
        weather data using the OpenWeather API. It displays weather conditions, 
        temperature, humidity, and wind speed in a visually appealing format.

        Parameters:
        - cities (list): A list of city names to choose from.
        - api_key (str): The API key for accessing the OpenWeather API.
        - country_code (str): The country code for the selected city.
        """
    # Sidebar for city selection
        with st.sidebar:
            st.markdown("## 🌍 Choose Your City")
            selected_city = st.selectbox("Select a city:", cities)
            
        if selected_city:
            current_weather = fetch_current_weather_data(selected_city, api_key, country_code)
            if current_weather:
                st.subheader(f"Current Weather in {selected_city}")

                # creating a weather condition icon and clear layout
                col1, col2 = st.columns([2, 3])
                with col1:
                    st.subheader(current_weather['weather_condition'])
                    st.image(f"https://openweathermap.org/img/wn/{current_weather['icon']}.png", width=100)
                with col2:
                    st.metric(label="🌡️ Temperature", value=f"{current_weather['temperature']} °C", delta=None)
                    st.metric(label="💧 Humidity", value=f"{current_weather['humidity']}%", delta=None)
                    st.metric(label="🌬️ Wind Speed", value=f"{current_weather['wind_speed']} m/s", delta=None)

                st.markdown("""
                    <style>
                    .weather-description {
                        font-family: 'Courier New', monospace;
                        font-size: 24px;
                        color: #FFD700;
                        text-shadow: 1px 1px 2px black;
                        margin-bottom: 15px;
                    }
                    .temp-high-low {
                        font-family: 'Arial', sans-serif;
                        font-size: 20px;
                        color: white;
                        background-color: rgba(0, 0, 0, 0.6);
                        padding: 10px;
                        border-radius: 10px;
                        box-shadow: 2px 2px 8px rgba(0, 0, 0, 0.4);
                        display: inline-block;
                        margin: 15px 0;
                    }
                    .temp-high {
                        color: #FF6347; /*  Red for high temperature */
                        font-weight: bold;
                    }
                    .temp-low {
                        color: #1E90FF; /*  Blue for low temperature */
                        font-weight: bold;
                    }
                    </style>
                    """, unsafe_allow_html=True)
                
                # Create two columns for the layout
                col1, col2 = st.columns([2,3])

                # column 1: Weather Description
                with col1:
                    st.markdown(f"""
                        <div class="weather-description">
                            🌤️ Description: {current_weather['description'].capitalize()}
                        </div>
                        """, unsafe_allow_html=True)

                # column 2: High and Low Temperature
                with col2:
                    st.markdown(f"""
                        <div class="temp-high-low">
                            🌡️ High: <span class="temp-high">{current_weather['high']}°C</span> | 
                            Low: <span class="temp-low">{current_weather['low']}°C</span>
                        </div>
                        """, unsafe_allow_html=True)     
            else:
                st.warning(f"Unable to fetch current weather for {selected_city}")

def weather_analysis(cur):
    """
    Displays a weather analysis dashboard based on the weather data.

    This function fetches weather data, allows the user to filter the data 
    by date, and displays various weather analysis results, such as 
    distinct weather conditions and temperature comparisons across cities.

    Parameters:
    - cur: A Snowflake cursor object used to execute SQL queries and fetch data.
    """
    
    st.title("Weather Analysis Dashboard")
    st.sidebar.title("Filter Options")

    weather_df = fetch_denormalized_into_df(st, cur)
    if weather_df is None or weather_df.empty:
        st.error("No data available to analyze.")
        return  
    
    # creating an instance of WeatherAnalysis class
    weather_analysis = WeatherAnalysis(weather_df)

    # sidebar date filters
    start_date = st.sidebar.date_input("Start Date", value=pd.to_datetime('2024-01-01'))
    end_date = st.sidebar.date_input("End Date", value=datetime.now() + timedelta(days=1))
    
    # filtering data based on selected date period
    filtered_df = weather_analysis.filter_by_date(start_date, end_date)

    # uUtility function for rendering the result or message
    def display_result(result, label):
        st.write(f"#### {label} ")
        if isinstance(result, pd.DataFrame) and not result.empty:
            st.dataframe(result, hide_index=True)
        elif isinstance(result, str):
            st.write(result)  # Display string message if applicable
        else:
            st.error("No valid data to display for this analysis.")  

    # Analysis outputs (compact & modularized)
    analysis_tasks = [
        ("Distinct weather conditions observed", weather_analysis.distinct_weather_conditions, filtered_df),
        ("Most common weather conditions per city", weather_analysis.rank_common_weather_conditions, filtered_df),
        ("Average temperature per city", weather_analysis.average_temperature, filtered_df),
        ("City with the highest absolute temperature", weather_analysis.max_temperature, filtered_df),
        ("City with the highest daily temperature variation", weather_analysis.highest_daily_temp_variation, filtered_df),
        ("City with the strongest wind", weather_analysis.strongest_wind, filtered_df),
        ("Comparing weather metrics between cities", weather_analysis.weather_metrics_comparison, filtered_df),
        ("Comparing rainy days between cities", weather_analysis.rainy_days_comparison, filtered_df),
        ("Strongest Winds per city", weather_analysis.strongest_winds_per_city, filtered_df),
        ("Daytime vs Nighttime temperatures", weather_analysis.day_vs_night_temperature, filtered_df),
        ("Temperature spikes (> 40°C)", weather_analysis.temp_spikes, filtered_df),
        ("Average cloud coverage per city", weather_analysis.avg_cloud_coverage, filtered_df)
    ]
    
    # looping through each task and displaying results
    for label, analysis_fn, df in analysis_tasks:
        result = analysis_fn(df) if df is not None else analysis_fn()
        display_result(result, label)
