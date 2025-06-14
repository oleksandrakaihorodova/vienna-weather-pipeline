import requests
import pandas as pd
import sqlite3
import os
from datetime import datetime


def fetch_weather():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 48.21,
        "longitude": 16.37,
        "hourly": "temperature_2m",
        "start": datetime.utcnow().strftime("%Y-%m-%dT%H:%M"),
        "end": (datetime.utcnow().replace(hour=23, minute=0)).strftime("%Y-%m-%dT%H:%M"),
        "timezone": "Europe/Vienna"
    }
    response = requests.get(url, params=params)
    data = response.json()
    return pd.DataFrame({
        "timestamp": data["hourly"]["time"],
        "temperature": data["hourly"]["temperature_2m"]
    })


def save_to_db(df):
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    os.makedirs("db", exist_ok=True)  # ensure the db directory exists
    conn = sqlite3.connect("db/weather_data.db")
    df.to_sql("vienna_weather", conn, if_exists="replace", index=False)
    conn.close()

if __name__ == "__main__":
    df = fetch_weather()
    save_to_db(df)
    print("saved.")
