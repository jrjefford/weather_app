#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import csv
from datetime import datetime
import json

def fetch_weather(latitude=51.5074, longitude=-0.1278):
    """
    Fetch current weather data from Open-Meteo API for specified coordinates.
    
    This function makes an API call to Open-Meteo to get current weather data
    including temperature and humidity for the given latitude and longitude.
    
    Args:
        latitude (float): Latitude coordinate (default: 51.5074 for London)
        longitude (float): Longitude coordinate (default: -0.1278 for London)
    
    Returns:
        dict: Parsed weather data with timestamp, temperature, humidity or None if error
    """
    # Define the Open-Meteo API endpoint
    base_url = "https://api.open-meteo.com/v1/forecast"
    
    # Set up API parameters
    # - latitude/longitude: coordinates for London
    # - hourly: request temperature_2m and relativehumidity_2m data
    # - current_weather: get current weather conditions
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'hourly': 'temperature_2m,relativehumidity_2m',
        'current_weather': 'true'
    }
    
    try:
        # Display API request details for debugging
        print(f"Making API request to: {base_url}")
        print(f"Parameters: {params}")
        
        # Make the HTTP GET request to the API
        response = requests.get(base_url, params=params)
        print(f"Response status code: {response.status_code}")
        
        # Check if the API request was successful
        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
            
            # Extract current weather data from the response
            current_weather = data.get('current_weather', {})
            
            # Get timestamp from current weather data
            # Convert from ISO format to readable format
            timestamp_str = current_weather.get('time', '')
            if timestamp_str:
                # Parse ISO timestamp and format it as YYYY-MM-DD HH:MM:SS
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
            else:
                # Fallback to current time if no timestamp in response
                formatted_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Extract temperature from current weather (in Celsius)
            temperature = current_weather.get('temperature', 'N/A')
            
            # Extract humidity from hourly data
            # Note: current_weather doesn't include humidity, so we get it from hourly data
            hourly_data = data.get('hourly', {})
            hourly_humidity = hourly_data.get('relativehumidity_2m', [])
            
            # Get the most recent humidity value (first in the array)
            humidity = hourly_humidity[0] if hourly_humidity else 'N/A'
            
            # Create the parsed weather data dictionary
            weather_data = {
                'timestamp': formatted_timestamp,
                'temperature': temperature,
                'humidity': humidity
            }
            
            return weather_data
            
        else:
            # Handle API errors
            print(f"API request failed with status code: {response.status_code}")
            print(f"Response: {response.text}")
            return None
        
    except requests.exceptions.RequestException as e:
        # Handle network/request errors
        print(f"Error fetching weather data: {e}")
        return None
    except json.JSONDecodeError as e:
        # Handle JSON parsing errors
        print(f"Error parsing JSON response: {e}")
        return None
    except KeyError as e:
        # Handle missing data in API response
        print(f"Error parsing weather data - missing key: {e}")
        return None
    except Exception as e:
        # Handle any other unexpected errors
        print(f"Unexpected error in fetch_weather: {e}")
        return None

def write_csv(weather_data, filename="weather.csv"):
    """
    Write weather data to a CSV file.
    
    This function takes the parsed weather data and writes it to a CSV file
    with the specified filename. The CSV will have columns: timestamp, temperature, humidity.
    
    Args:
        weather_data (dict): Parsed weather data with timestamp, temperature, humidity
        filename (str): Output CSV filename (default: "weather.csv")
    
    Returns:
        bool: True if successful, False if error
    """
    # Check if we have valid weather data to write
    if not weather_data:
        print("No weather data to save.")
        return False
    
    try:
        # Open the CSV file for writing
        # Use 'w' mode to overwrite existing file
        # Set newline='' to prevent extra blank lines
        # Set encoding='utf-8' for proper character support
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            # Define the column names for the CSV
            fieldnames = ['timestamp', 'temperature', 'humidity']
            
            # Create a CSV writer object with the specified fieldnames
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write the header row with column names
            writer.writeheader()
            
            # Write the weather data as a single row
            writer.writerow(weather_data)
        
        # Display success message with the data that was saved
        print(f"Weather data saved to {filename}")
        print(f"Timestamp: {weather_data['timestamp']}")
        print(f"Temperature: {weather_data['temperature']}°C")
        print(f"Humidity: {weather_data['humidity']}%")
        
        return True
        
    except Exception as e:
        # Handle any errors during file writing
        print(f"Error saving to CSV: {e}")
        return False

def main():
    """
    Main function that orchestrates the weather data fetching and CSV writing process.
    
    This function:
    1. Fetches weather data for London using the Open-Meteo API
    2. Writes the data to a CSV file
    3. Handles errors and provides appropriate exit codes
    """
    # Define London coordinates
    latitude = 51.5074   # London latitude
    longitude = -0.1278  # London longitude
    
    print(f"Fetching current weather data for London (lat: {latitude}, lon: {longitude})...")
    
    try:
        # Step 1: Fetch weather data from the API
        # This calls the fetch_weather function with London coordinates
        weather_data = fetch_weather(latitude, longitude)
        
        # Check if weather data was successfully fetched
        if weather_data:
            # Step 2: Write the weather data to CSV file
            # This calls the write_csv function to save the data
            success = write_csv(weather_data)
            
            # Check if CSV writing was successful
            if success:
                print("Weather data successfully fetched and saved to CSV.")
            else:
                # Exit with error code 1 if CSV writing failed
                print("ERROR: Failed to save weather data to CSV.")
                exit(1)
        else:
            # Exit with error code 1 if weather data fetching failed
            print("ERROR: Failed to fetch weather data from API.")
            exit(1)
            
    except Exception as e:
        # Handle any unexpected errors in the main process
        print(f"ERROR: Unexpected error occurred: {e}")
        exit(1)

# Entry point of the script
# This ensures main() only runs when the script is executed directly
# (not when imported as a module)
if __name__ == "__main__":
    main()