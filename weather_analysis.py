import pandas as pd
import numpy as np

class WeatherAnalysis:
    def __init__(self, df):
        """
        Initializes the WeatherAnalysis object with a copy of the input DataFrame.
        Ensures that the 'OBSERVATION_TIME' column is converted to datetime.

        Args:
            df (pd.DataFrame): The input DataFrame containing weather data.
        """
        
        self.df=df.copy()
        self.df['OBSERVATION_TIME'] = pd.to_datetime(self.df['OBSERVATION_TIME'])

    def filter_by_date(self, start_date, end_date):
        """
            Filters the DataFrame for records between the start and end dates.

            Args:
                start_date (str or pd.Timestamp): The start date for filtering (inclusive).
                end_date (str or pd.Timestamp): The end date for filtering (inclusive).

            Returns:
                pd.DataFrame: Filtered DataFrame containing rows between the specified dates.
        """
        
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        return self.df[(self.df['OBSERVATION_TIME'] >= start_date) & (self.df['OBSERVATION_TIME'] <= end_date)]

    #1
    def distinct_weather_conditions(self, filtered_df):
        """
        Returns the count of distinct weather conditions observed in the DataFrame.
        Args:
            filtered_df (pd.DataFrame): The filtered DataFrame after date filtering.
        Returns:
            str: A message with the number of distinct weather conditions or an error message if the data is unavailable.
        """

        if filtered_df.empty:
            return "No data available for the selected date range"
        
        unique_conditions_count = filtered_df['WEATHER_CONDITION'].nunique()
        
        if unique_conditions_count == 0:
            return "No distinct weather conditions found for the selected date range"
        
        return f"The number of distinct weather conditions observed is **{str(unique_conditions_count)}**"

    # 2
    def rank_common_weather_conditions(self, filtered_df):
        """
        Provides the count of occurrences of different weather conditions grouped by city.
        Args:
            filtered_df (pd.DataFrame): The filtered DataFrame.
        Returns:
            pd.DataFrame: A DataFrame with the weather condition count grouped by city.
        """

        if filtered_df.empty:
            return "No data available for the selected date range"

        try:
            # Group by city and weather condition, count occurrences
            weather_counts = (filtered_df.groupby(['CITY_NAME', 'WEATHER_CONDITION'])
                            .size()
                            .reset_index(name='COUNT'))

            if weather_counts.empty:
                return "No weather conditions found for the selected date range"

            # Rank the weather conditions within each city
            weather_counts['RANK'] = weather_counts.groupby('CITY_NAME')['COUNT'].rank(method='dense', ascending=False)

            # Sort the DataFrame by city and rank
            ranked_conditions = weather_counts.sort_values(by=['CITY_NAME', 'RANK'])

            return ranked_conditions

        except Exception as e:
            return f"An error occurred: {str(e)}"

    # 3
    def average_temperature(self, filtered_df):
        """
        Returns the average temperature per city.

        Args:
            filtered_df (pd.DataFrame): The filtered DataFrame.

        Returns:
            pd.DataFrame: A DataFrame with the average temperature for each city.
        """
        if filtered_df.empty:
            return "No data available for the selected date range"

        try:
            return (filtered_df.groupby('CITY_NAME')['TEMPERATURE']
                    .mean()
                    .round(2)
                    .reset_index(name='AVG_TEMPERATURE'))
        except Exception as e:
            return f"An error occurred: {str(e)}"

    # 4
    def max_temperature(self, filtered_df):
        """
        Returns the city with the highest temperature recorded in the filtered data.

        Args:
            filtered_df (pd.DataFrame): The filtered DataFrame.

        Returns:
            str: A message with the city and the highest recorded temperature.
        """
        
        if filtered_df.empty:
            return "No data available for the selected date range"

        try:
            max_temp_index = filtered_df['TEMPERATURE'].idxmax()
            city = filtered_df.loc[max_temp_index, 'CITY_NAME']
            temperature = filtered_df.loc[max_temp_index, 'TEMPERATURE']
            
            return f"The city with the highest temperature is **{city}** with a temperature of **{temperature}** °C."
        except Exception as e:
            return f"Error: {str(e)}"

    # 5
    def highest_daily_temp_variation(self, filtered_df):
        """
        Identifies the city with the highest temperature variation (max-min) in a day.

        Args:
            filtered_df (pd.DataFrame): The filtered DataFrame.

        Returns:
            pd.DataFrame: A DataFrame with the city name and temperature variation.
        """
        
        if filtered_df.empty:
            return "No data available for the selected date range"

        try:
            # checking for required columns
            required_columns = ['CITY_NAME', 'TEMP_MAX', 'TEMP_MIN', 'OBSERVATION_TIME']
            if not all(col in filtered_df.columns for col in required_columns):
                return f"Missing columns in the DataFrame. Required columns are: {required_columns}"


            # calculating temperature variation for each city
            filtered_df['TEMP_VARIATION'] = filtered_df['TEMP_MAX'] - filtered_df['TEMP_MIN']

            # Group by city and calculate mean temperature variation
            daily_temp_variations = (filtered_df.groupby('CITY_NAME', as_index=False)
                                    .agg({'TEMP_VARIATION': 'mean'}))

            # Rank the temperature variations
            daily_temp_variations['variation_rank'] = daily_temp_variations['TEMP_VARIATION'].rank(method='max', ascending=False)

            # Select the city with the highest temperature variation
            highest_variation = daily_temp_variations[daily_temp_variations['variation_rank'] == 1]

            if highest_variation.empty:
                return "No daily temperature variation found for the selected date range"

            return highest_variation[['CITY_NAME', 'TEMP_VARIATION']] 

        except Exception as e:
            return f"An error occurred: {str(e)}"

    # 6
    def strongest_wind(self, filtered_df):
        """
        Returns the city with the strongest wind speed recorded.

        Args:
            filtered_df (pd.DataFrame): The filtered DataFrame.

        Returns:
            str: A message with the city and the highest wind speed.
        """

        if filtered_df.empty:
            return "No data available for the selected date range"

        try:
            if filtered_df['WIND_SPEED'].isnull().all():
                return "No valid wind speed data available."

            max_wind_index = filtered_df['WIND_SPEED'].idxmax()
            city = filtered_df.loc[max_wind_index, 'CITY_NAME']
            wind_speed = filtered_df.loc[max_wind_index, 'WIND_SPEED']
            
            return f"The city with the strongest wind is **{city}** with a wind speed of **{wind_speed}** m/s."
        except Exception as e:
            return f"Error: {str(e)}"

    # 7 
    def weather_metrics_comparison(self,filtered_df):
        """
        Compares weather metrics (temperature, humidity, and wind speed) across cities.

        Args:
            filtered_df (pd.DataFrame): The filtered DataFrame.

        Returns:
            pd.DataFrame: A DataFrame with the average metrics for each city.
        """
        
        try:
            if filtered_df.empty:
                return "No data available for the selected date range"

            metrics = filtered_df.groupby('CITY_NAME').agg(
                AVG_TEMPERATURE=('TEMPERATURE', 'mean'),
                AVG_HUMIDITY=('HUMIDITY', 'mean'),
                AVG_WIND_SPEED=('WIND_SPEED', 'mean')
            ).round(2).reset_index()

            if metrics.empty or metrics.shape[0] == 0:
                return "No weather metrics available for the given period."
            
            return metrics

        except Exception as e:
            return pd.DataFrame(columns=['ERROR']).assign(ERROR=str(e))

    # 8
    def rainy_days_comparison(self,filtered_df):
        """
        Counts the number of rainy days for each city.

        Args:
            filtered_df (pd.DataFrame): The filtered DataFrame.

        Returns:
            pd.DataFrame: A DataFrame with the count of rainy days per city.
        """
        
        if filtered_df.empty:
            return "No data available for the selected date range"

        try:
            rainy_days = (filtered_df[filtered_df['RAIN'] > 0]
                        .groupby(['CITY_NAME', filtered_df['OBSERVATION_TIME'].dt.date])
                        .size()
                        .reset_index(name='RAINY_DAYS')
                        .groupby('CITY_NAME')['RAINY_DAYS']
                        .count()
                        .reset_index())

            if rainy_days.empty:
                return "No data available for the selected date range"

            return rainy_days
        except Exception as e:
            return pd.DataFrame(columns=['ERROR']).assign(ERROR=str(e))

    # 9
    def strongest_winds_per_city(self, filtered_df):
        """
        Returns the maximum wind speed recorded for each city.

        Args:
            filtered_df (pd.DataFrame): The filtered DataFrame.

        Returns:
            pd.DataFrame: A DataFrame with the maximum wind speed for each city.
        """
        
        if self.df.empty:
            return "No data available to calculate wind speed."

        try:
            if filtered_df.empty:
                return "No data available for the selected date range"

            max_wind_speed = (filtered_df.groupby('CITY_NAME')['WIND_SPEED'].max()
                            .reset_index(name='MAX_WIND_SPEED'))
            
            # ordering by max wind speed
            max_wind_speed = max_wind_speed.sort_values(by='MAX_WIND_SPEED', ascending=False)
            
            return max_wind_speed
        except Exception as e:
            return pd.DataFrame(columns=['ERROR']).assign(ERROR=str(e))

    # 10
    def day_vs_night_temperature(self, filtered_df):
        """
        Compares the average temperature during the day and night for each city.

        Args:
            filtered_df (pd.DataFrame): The filtered DataFrame.

        Returns:
            pd.DataFrame: A DataFrame showing the average day and night temperature for each city.
        """
        
        if filtered_df.empty:
            return "No data available for the selected date range"

        filtered_df['DAY_NIGHT'] = np.where(filtered_df['OBSERVATION_TIME'].dt.hour.between(6, 18), 'Day', 'Night')
        
        return (filtered_df.groupby(['CITY_NAME', 'DAY_NIGHT'])['TEMPERATURE']
                .mean()
                .round(2)
                .reset_index(name='AVG_TEMPERATURE'))

    # 11
    def temp_spikes(self, filtered_df):
        """
        Identifies temperature spikes where the temperature exceeds 40°C.

        Args:
            filtered_df (pd.DataFrame): The filtered DataFrame.

        Returns:
            pd.DataFrame: A DataFrame with cities and timestamps where the temperature exceeds 40°C.
        """
        
        if filtered_df.empty:
            return "No data available for the selected date range"

        try:
            spikes_df = filtered_df[filtered_df['TEMPERATURE'] > 40][['CITY_NAME', 'OBSERVATION_TIME', 'TEMPERATURE']]

            if spikes_df.empty:
                return "No temperature spikes found above 40 degrees."

            return spikes_df
        except Exception as e:
            return pd.DataFrame(columns=['ERROR']).assign(ERROR=str(e))

    # 12
    def avg_cloud_coverage(self,filtered_df):
        """
        Returns the average cloud coverage for each city.

        Args:
            filtered_df (pd.DataFrame): The filtered DataFrame.

        Returns:
            pd.DataFrame: A DataFrame with the average cloud coverage per city.
        """
        
        if filtered_df.empty:
            return "No data available for the selected date range"

        try:
            avg_cloud_coverage_df = filtered_df.groupby('CITY_NAME')['CLOUDS'].mean().round(2).reset_index(name='AVG_CLOUD_COVERAGE')

            # sorting the result by avg_cloud_coverage in descending order
            avg_cloud_coverage_df = avg_cloud_coverage_df.sort_values(by='AVG_CLOUD_COVERAGE', ascending=False)

            if avg_cloud_coverage_df.empty:
                return "No cloud coverage data found."

            return avg_cloud_coverage_df
        except Exception as e:
            return pd.DataFrame(columns=['ERROR']).assign(ERROR=str(e))

