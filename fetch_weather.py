"""
Daily weather data fetcher using the Open-Meteo API (free, no API key required).
Appends the current day's forecast to data/weather_data.csv.
"""

import csv
import os
import sys
from datetime import datetime

import requests

# Location: Mexico City (change as needed)
LATITUDE = 19.4326
LONGITUDE = -99.1332
TIMEZONE = "America/Mexico_City"
CSV_FILE = "data/weather_data.csv"

API_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_weather() -> dict:
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "current_weather": True,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max",
        "timezone": TIMEZONE,
        "forecast_days": 1,
    }

    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def parse_data(api_response: dict) -> dict:
    current = api_response["current_weather"]
    daily = api_response["daily"]

    return {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "fetched_at": datetime.now().strftime("%H:%M:%S UTC"),
        "temperature_c": current["temperature"],
        "windspeed_kmh": current["windspeed"],
        "weathercode": current["weathercode"],
        "max_temp_c": daily["temperature_2m_max"][0],
        "min_temp_c": daily["temperature_2m_min"][0],
        "precipitation_mm": daily["precipitation_sum"][0],
        "max_windspeed_kmh": daily["windspeed_10m_max"][0],
    }


def save_to_csv(row: dict) -> None:
    os.makedirs(os.path.dirname(CSV_FILE), exist_ok=True)
    file_exists = os.path.isfile(CSV_FILE)

    with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def main() -> None:
    print(f"Fetching weather data for ({LATITUDE}, {LONGITUDE})...")

    api_response = fetch_weather()
    row = parse_data(api_response)
    save_to_csv(row)

    print("Saved row:")
    for key, value in row.items():
        print(f"  {key}: {value}")
    print(f"\nData appended to {CSV_FILE}")


if __name__ == "__main__":
    try:
        main()
    except requests.RequestException as e:
        print(f"Error calling API: {e}", file=sys.stderr)
        sys.exit(1)
