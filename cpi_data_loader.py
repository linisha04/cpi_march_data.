import psycopg2
import pandas as pd

# Database connection details
hostname = "localhost"
database = "final"
username = "postgres" 
# tata_postgres
#ssh -L 9000:127.0.0.1:8000 tata_user@34.133.113.52
port_id = 5432
password = "admin"

cur = None
conn = None

# Load the CSV data
df = pd.read_csv("cpi_data_march_2025.csv")
df.columns = df.columns.str.strip()

# Convert 'Month' to a numeric column
month_mapping = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12
}
df["month_numeric"] = df["Month"].map(month_mapping)

# Replace '*' with 0 in numeric columns
# df["Index"] = df["Index"].replace('*', 0).astype(float)
# df["Inflation (%)"] = df["Inflation (%)"].replace('*', 0).astype(float)

df["Index"] = df["Index"].replace("*", None)
df["Inflation (%)"] = df["Inflation (%)"].replace("*", None)
df['data_source'] = "Price Statistics Division, MoSPI"
# Debugging: Check if the DataFrame is empty
print(df.head())  
print(df.columns)
print(f"Total rows to insert: {len(df)}")

try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(host=hostname, dbname=database, user=username, password=password, port=port_id)
    cur = conn.cursor()

    # Create table with 'month_numeric' column
    create_script = """
    CREATE TABLE IF NOT EXISTS cpi_inflation_data (
        id SERIAL PRIMARY KEY,
        base_year INT,
        year INT,
        month VARCHAR(20),
        month_numeric INT,
        state VARCHAR(50),
        sector VARCHAR(50),
        group_name VARCHAR(100),
        sub_group_name VARCHAR(100),
        inflation_index DECIMAL(10, 2),
        inflation_rate DECIMAL(5, 2),
        data_release_date VARCHAR(100),
        data_updated_date  VARCHAR(100),
        data_source VARCHAR(100),
        UNIQUE (year, month, state, sector, group_name, sub_group_name, inflation_index, inflation_rate)
    );
    """
    cur.execute(create_script)
    conn.commit()

    # Ensure DataFrame is not empty
    if not df.empty:
        # Insert data into the table
        insert_query = """ 
        INSERT INTO cpi_inflation_data (base_year, year, month, month_numeric, state, sector, group_name, sub_group_name, inflation_index, inflation_rate ,data_release_date,data_updated_date,data_source ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s ,%s ,%s)
        ON CONFLICT (year, month, state, sector, group_name, sub_group_name, inflation_index, inflation_rate) DO NOTHING;
        """
        # Prepare records for insertion
        records = df[['BaseYear', 'Year', 'Month', 'month_numeric', 'State', 'Sector', 'Group', 'SubGroup', 'Index', 'Inflation (%)' ,'data_release_date','data_updated_date','data_source']].values.tolist()
        cur.executemany(insert_query, records)
        conn.commit()

        print("Data inserted successfully!")
    else:
        print(" DataFrame is empty! No records inserted.")

except Exception as error:
    print("Error:", error)

finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
