import requests
import json
import psycopg2
import pandas as pd

# PostgreSQL Connection Details
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "your_password",
    "host": "localhost",
    "port": "5432"
}

# OpenWeather API Details
API_KEY = "5443096717a230028174f3b752ed1c93"
CITIES = ["London", "New York", "Tokyo", "Sydney", "Paris"]  # Add more cities here

# Connect to PostgreSQL
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

for city in CITIES:
    URL = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(URL)

    if response.status_code == 200:
        data = response.json()

        # Extract required fields
        weather_info = {
            "city": data.get("name", "Unknown"),
            "temperature": data["main"].get("temp"),
            "humidity": data["main"].get("humidity"),
            "weather_description": data["weather"][0].get("description", "No description")
        }

        # Convert to DataFrame for cleaning
        df = pd.DataFrame([weather_info])

        # Handle missing values
        df.fillna({"temperature": 0.0, "humidity": 0, "weather_description": "Unknown"}, inplace=True)

        # Convert 'temperature' and 'humidity' to native Python types
        temperature = float(df["temperature"].values[0])  # Convert to native float
        humidity = int(df["humidity"].values[0])  # Convert to native int
        weather_description = df["weather_description"].values[0]

        # Check for duplicate data before inserting
        cursor.execute("SELECT COUNT(*) FROM weather_data WHERE city = %s", (weather_info["city"],))
        existing_count = cursor.fetchone()[0]

        if existing_count == 0:
            # Insert data into the database
            cursor.execute("""
                INSERT INTO weather_data (city, temperature, humidity, weather_description)
                VALUES (%s, %s, %s, %s)
            """, (
                weather_info["city"],
                temperature,
                humidity,
                weather_description
            ))

            conn.commit()
            print(f"Weather data for {city} inserted successfully!")
        else:
            print(f"Data for {city} already exists. Skipping insertion.")

    else:
        print(f"Error fetching data for {city}: {response.status_code} {response.text}")

# Close connection
cursor.close()
conn.close()
