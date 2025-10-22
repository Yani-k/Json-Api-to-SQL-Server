import requests
import pyodbc
from datetime import datetime

# Step 1: Fetch and clean data
url = "https://data.ny.gov/resource/wujg-7c2s.json"
response = requests.get(url)
raw_data = response.json()

def clean_record(rec):
    return {k: v for k, v in rec.items() if not k.startswith(":@computed_region") and k != "georeference"}

# Step 2: Transform records
def transform_record(rec):
    return (
        datetime.fromisoformat(rec["transit_timestamp"].replace("T", " ").split(".")[0]),
        rec.get("transit_mode", ""),
        rec.get("station_complex_id", ""),
        rec.get("station_complex", ""),
        rec.get("borough", ""),
        rec.get("payment_method", ""),
        rec.get("fare_class_category", ""),
        int(float(rec.get("ridership", "0"))),
        int(float(rec.get("transfers", "0"))),
        float(rec.get("latitude", "0")),
        float(rec.get("longitude", "0"))
    )

cleaned_data = [transform_record(clean_record(rec)) for rec in raw_data]

# Step 3: Connect to SQL Server
cnxn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
        "Server=TYUNUS\\SQLEXPRESS;"
        "Database=mta_api;"
        "Trusted_connection=yes;"
)
cursor=cnxn.cursor()

# Step 4: Insert into staging table
insert_query = """
INSERT INTO dbo.Staging_SubwayRidership (
    transit_timestamp,
    transit_mode,
    station_complex_id,
    station_complex,
    borough,
    payment_method,
    fare_class_category,
    ridership,
    transfers,
    latitude,
    longitude
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

for record in cleaned_data:
    cursor.execute(insert_query, record)

cnxn.commit()
cursor.close()
cnxn.close()
