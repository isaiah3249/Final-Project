import sqlite3
import pandas as pd

# Step 1: Load raw Excel file into a pandas DataFrame
raw_data_path = 'F1-15 Percent of Flight Delay by Delay Cause 2010-2022.xlsx'
data = pd.read_excel(raw_data_path, header=1)

# Transpose the dataset to match prior steps
data = data.set_index('Unnamed: 0').T

# Inspect the number of columns dynamically
print(f"Detected {len(data.columns)} columns. Assigning names dynamically...")

# Dynamically generate column names
column_names = [
    'Number of Arriving Flights (millions)', 'Air Carrier Delay',
    'Aircraft Arriving Late', 'National Aviation System Delay',
    'Security Delay', 'Extreme Weather', 'Other Causes',
    'Column 8', 'Column 9', 'Column 10'
]

# Adjust column names to match the detected number of columns
if len(data.columns) > len(column_names):
    column_names += [f"Extra Column {i}" for i in range(len(column_names), len(data.columns))]

# Assign the dynamically adjusted column names
data.columns = column_names[:len(data.columns)]

# Drop unnecessary columns
columns_to_drop = ['Number of Arriving Flights (millions)', 'Extreme Weather', 'Other Causes']
data = data.drop(columns=columns_to_drop, errors='ignore')

# Convert all columns to numeric
data = data.apply(pd.to_numeric, errors='coerce')

# Step 2: Create SQLite database in memory and insert the dataset
conn = sqlite3.connect("flight_delays.db")
data.to_sql('raw_data', conn, if_exists='replace', index=False)

# Step 3: Use SQL to clean the data
query = """
SELECT 
    `Air Carrier Delay`, 
    `Aircraft Arriving Late`, 
    `National Aviation System Delay`, 
    `Security Delay`
FROM 
    raw_data
WHERE 
    `Air Carrier Delay` IS NOT NULL
    AND `Aircraft Arriving Late` IS NOT NULL
    AND `National Aviation System Delay` IS NOT NULL
    AND `Security Delay` IS NOT NULL
"""
cleaned_data = pd.read_sql_query(query, conn)

# Step 4: Save the cleaned data to a new CSV file
cleaned_data_path = 'cleaned_flight_delay_data.csv'
cleaned_data.to_csv(cleaned_data_path, index=False)
print(f"Cleaned data saved to: {cleaned_data_path}")

# Step 5: Optional - Verify the cleaned dataset
print("Cleaned Data Preview:")
print(cleaned_data.head())

# Close the database connection
conn.close()
