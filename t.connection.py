import pyodbc
import pandas as pd
cnxn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
        "Server=TYUNUS\\SQLEXPRESS;"
        "Database=mta_api;"
        "Trusted_connection=yes;"
)

cursor=cnxn.cursor()

cursor.execute("SELECT * FROM Staging_SubwayRidership")

df=pd.read_sql("SELECT * FROM Staging_SubwayRidership", cnxn)

print(df)

