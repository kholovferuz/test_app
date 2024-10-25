import streamlit as st
from snowflake_connection import cur
from pages import snowflake_tables, current_weather, weather_analysis
from data_orchestration.dags.modules.api_calls import cities_list
import os


# Streamlit App
def main():
    """
    The main function to run the Streamlit Weather App.

    This function configures the Streamlit app's layout and sidebar, allowing 
    users to navigate between different pages: Current Weather, Weather 
    Analysis, and Snowflake Tables. Based on the user's selection, the 
    corresponding function is called to display the relevant content.

    Pages:
    - Current Weather: Displays current weather information for selected cities.
    - Weather Analysis: Provides insights and analysis based on historical weather data.
    - Snowflake Tables: Displays data stored in Snowflake tables related to weather.

    It initializes the app with a wide layout and a custom page title and icon.
    """
    
    st.set_page_config(
        page_title="Weather app",
        page_icon="🌤️",
        layout="wide")
    # creating a link for official documentation in sidebar
    st.sidebar.markdown('[Official documentation](http://app-documentation-demo.s3-website.eu-central-1.amazonaws.com/snowflake_connection.html)')

    selected_page = st.sidebar.selectbox("Select a page", ["Current Weather","Weather Analysis", "Snowflake Tables"])
    if selected_page == "Weather Analysis":
        weather_analysis(cur)
    elif selected_page=='Snowflake Tables':
        snowflake_tables(cur)
    elif selected_page == "Current Weather":
        current_weather(cities_list, os.getenv('API_KEY'), os.getenv('COUNTRY_CODE'))
    else:
        st.error("Invalid page selection.")

if __name__ == "__main__":
    main()
