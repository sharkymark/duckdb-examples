import duckdb
import subprocess
import os
import argparse  # Import the argparse module
import requests
import csv
import tempfile
from io import StringIO

# Set up argument parsing
parser = argparse.ArgumentParser(description="Create Northwind tables in DuckDB.")
parser.add_argument("--db", default=":memory:", help="Path to the DuckDB database file.  Defaults to in-memory.")
args = parser.parse_args()

# Use the database file from the argument
db_path = args.db
try:
    # Try to connect to the provided database path to check if it is a valid path.
    con = duckdb.connect(database=db_path, read_only=False)
    print(f"Using database: {db_path}")
except Exception as e:
    print(f"Error connecting to {db_path}: {e}")
    print("Using an in-memory database instead.")
    db_path = ":memory:"
    con = duckdb.connect(database=db_path, read_only=False) # connect to memory if file doesn't work.


# Base URL for the Northwind CSV files
base_url = "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/"

# List of CSV files (table names)
csv_files = [
    "categories.csv",
    "customers.csv",
    "employees.csv",
    "order-details.csv",
    "orders.csv",
    "products.csv",
    "shippers.csv",
    "suppliers.csv"
]

def clean_csv(url, replace_char=' '):
    """
    Cleans a CSV by replacing commas in all fields with a space, merging extra fields.
    
    Args:
        url (str): CSV URL.
        replace_char (str): Replace commas with this (default: ' ').
    
    Returns:
        str: Path to cleaned CSV.
    """
    response = requests.get(url)
    csv_reader = csv.reader(StringIO(response.text))
    header = next(csv_reader)
    cleaned_rows = [header]
    expected_cols = len(header)
    
    for row in csv_reader:
        if len(row) == expected_cols:
            cleaned_row = [field.replace(',', replace_char) for field in row]
            cleaned_rows.append(cleaned_row)
        elif len(row) > expected_cols:
            cleaned_row = [field.replace(',', replace_char) for field in row[:expected_cols-1]]
            cleaned_row.append(replace_char.join(row[expected_cols-1:]))
            cleaned_rows.append(cleaned_row)
            print(f"Fixed row in {url}: {row} -> {cleaned_row}")
        else:
            cleaned_row = [field.replace(',', replace_char) for field in row]
            cleaned_rows.append(cleaned_row)
    
    temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='utf-8', newline='')
    writer = csv.writer(temp_file, quoting=csv.QUOTE_ALL)
    writer.writerows(cleaned_rows)
    temp_file_path = temp_file.name
    temp_file.close()
    return temp_file_path

def create_table_from_csv(table_name, csv_url):
    """
    Creates or replaces a table in DuckDB from a CSV file URL.

    Args:
        table_name (str): The name of the table to create.
        csv_url (str): The URL of the CSV file.
    """
    try:

        # Clean the CSV
        temp_file_path = clean_csv(csv_url)

        # Use a WITH STATEMENT for table creation.
        con.execute(f"""
            CREATE OR REPLACE TABLE "nw_{table_name}" AS
            SELECT * FROM read_csv('{temp_file_path}', delim=',', header=TRUE, quote='"', auto_detect=TRUE)
        """)

        print(f"Table '{table_name}' created or replaced successfully from '{csv_url}'")
        con.commit()  # Commit the transaction
        os.remove(temp_file_path)  # Clean up the temporary file
    except Exception as e:
        print(f"Error creating or replacing table 'nw_{table_name}' from '{csv_url}': {e}")

# Iterate through the CSV files and create tables
for csv_file in csv_files:
    table_name = os.path.splitext(csv_file)[0]  # Remove the ".csv" extension
    table_name = table_name.replace('-', '_')  # Replace hyphens with underscores
    csv_url = base_url + csv_file
    create_table_from_csv(table_name, csv_url)

# Verify the tables have been created (optional)
print("\nTables in the DuckDB database:")
# tables = con.execute("SHOW TABLES LIKE 'nw_'").fetchall()
tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'nw_%'").fetchall()
for table in tables:
    print(table[0])

# Example query (optional)
print("\nExample query: Selecting the first 5 rows from the 'Customers' table:")
try:
    result = con.execute("SELECT * FROM nw_customers LIMIT 5").fetchall()
    for row in result:
        print(row)
except Exception as e:
    print(f"Error running example query: {e}")

# Close the connection
con.close()
