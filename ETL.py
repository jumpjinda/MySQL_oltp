import pandas as pd
import pymysql
import subprocess


# Discovery csv data
# data columns
# - 1st column is product_id
# - 2nd column is customer_id
# - 3rd column is price
# - 4th column is quantity
# - 5th column is timestamp

df = pd.read_csv('dataset/oltpdata.csv', names=['product_id', 'customer_id', 'price', 'quantity', 'timestamp'], header=None)

# Convert timestamp column from object to datetime64
df['timestamp'] = df['timestamp'].astype('datetime64')

# Connect to MySQL server to create database and table
host = 'localhost'
port = 3306
user ='root'
password = 'root'
database = 'sales'

# Creates connection string
conn = pymysql.connect(host=host, user=user, password=password, autocommit=True)

# Create a cursor for execute
cur = conn.cursor()

# Test a connection
cur.execute("select @@version")
output = cur.fetchone() # fetchone() return tuple
print(f'MySQL version = {output[0]}')

try:
    cur.execute(f"DROP DATABASE IF EXISTS {database};")
except pymysql.err.ProgrammingError as e:
    print(e) 

try:
    cur.execute(f"CREATE DATABASE {database};")
except pymysql.err.ProgrammingError as e:
    print(e)

# DataFrame.to_sql will create new table for us
connection_string = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
df.to_sql('sales_data', connection_string, index=False)

# Checking that data has beed load to database
try:
    cur.execute("USE sales;")
    query = cur.execute("SELECT * FROM sales_data LIMIT 10;")
except pymysql.err.ProgrammingError as e:
    print(e)

result = cur.fetchall()
for row in result:
    print(row)


# Close connection
try:
    conn.close()
except pymysql.err.ProgrammingError as e:
    print(e)

# Dump table to file.sql
table = 'sales_data'

with open(f'dataset\{table}_dump.sql','w') as output:
    c = subprocess.Popen(['mysqldump', '-u', user, '-p%s' %password, database],
    stdout=output, shell=True)