import pandas as pd
from faker import Faker
from sqlalchemy import create_engine
import random
import string
import os
import pymysql
import json
pymysql.install_as_MySQLdb()

# Replace these with your RDS connection details
rds_username = 'root'
rds_password = 'qwerty1234'
rds_host = 'localhost'
rds_port = '3306'
rds_db_name = 'testdb1'
rds_table_name = 'emp'
# Create a SQLAlchemy engine to connect to the RDS database
engine = create_engine(f"mysql://{rds_username}:{rds_password}@{rds_host}:{rds_port}/{rds_db_name}")

'''
  Update this Variable for handling foreign key present with this table or columns 
  or which you want to use dependent data 
'''
foreign_and_dep_keys = {
    
    # Add your foreign key columns and lists of possible values
    }

def readJson(file_path):
    try:
        with open(file_path, 'r') as file:
            config = json.load(file)
        return (config)
    except FileNotFoundError:
        print("Config file not found.")
    except json.JSONDecodeError:
        print("Error decoding JSON.")


# Function to generate fake data based on column data types
def generate_fake_data(data_types, foreign_keys, batch_size,config):
    fake = Faker()
    generated_data = {}

    for column, data_type in data_types.items():
        # Skip columns that are auto-incremented primary keys
        if column.lower() == 'id':
            continue
        if 'auto_increment' in data_type.lower() and 'primary key' in data_type.lower():
            continue

        if column.lower() == 'empname':
            generated_data[column]=[random.choice(config["name"]) + ' ' + random.choice(config["name"])]
            continue

        if column.lower() == 'manager':
            generated_data[column]=[random.choice(config["name"]) + ' ' + random.choice(config["name"])]
            continue

        if column.lower() == 'dept':
            generated_data[column]=[random.choice(config["dept"])]
            continue

        if column.lower() == 'designation':
            generated_data[column]=[random.choice(config["designation"])]
            continue

        if column in foreign_keys:
            generated_data[column] = [random.choice(foreign_keys[column]) for _ in range(batch_size)]
        elif 'smallint' in data_type.lower():
            # Generate a wider range of random integers for SMALLINT
            c= [random.randint(-32768, 32767) for _ in range(batch_size)]  # Adjust the range as needed for SMALLINT
        elif 'tinyint' in data_type.lower() and '1' in data_type.lower():
            # Generate random boolean-like values for TINYINT(1)
            generated_data[column] = [random.choice([0, 1]) for _ in range(batch_size)]
        elif 'tinyint' in data_type.lower():
            # Generate random values within a wider range for TINYINT
            generated_data[column] = [random.randint(-128, 127) for _ in range(batch_size)]
        elif 'int' in data_type.lower() or 'bigint' in data_type.lower():
            # Generate a wider range of random integers for INT and BIGINT
            generated_data[column] = [random.randint(1, 10**9) for _ in range(batch_size)]  # Adjust the range as needed
        elif 'tinyblob' in data_type.lower():
            # Generate more randomized and shorter binary data for TINYBLOB
            generated_data[column] = [os.urandom(random.randint(1, 255)) for _ in range(batch_size)]  # Adjust the range of length as needed
        elif 'time' in data_type.lower():
            # Generate more varied and randomized time values for TIME
            generated_data[column] = [
                    fake.time(pattern='%H:%M:%S.%f')[:-3]  # Adjust the format as needed, removing the last three characters (microseconds)
                    for _ in range(batch_size)
                    ]
        elif 'date' in data_type.lower():
            # Generate random date values spanning a wider range for DATE
            generated_data[column] = [fake.date_between(start_date='-50y', end_date='today') for _ in range(batch_size)]
        elif 'mediumtext' in data_type.lower():
            # Generate unique random strings with Unicode characters for MEDIUMTEXT
            generated_data[column] = [''.join(random.choices(string.ascii_letters + string.digits + '\u4e00-\u9fa5', k=5000)) for _ in range(batch_size)]
        elif 'longtext' in data_type.lower():
            # Generate unique random strings with Unicode characters for LONGTEXT
            generated_data[column] = [
                ''.join(random.choices(
                    string.ascii_letters + string.digits + string.punctuation + string.whitespace + '\u4e00-\u9fa5',  # Include Unicode characters
                    k=random.randint(1, 5000))  # Adjust the maximum length as needed
                )
                for _ in range(batch_size)
            ]
        elif 'text' in data_type.lower():
            # Generate unique random strings with Unicode characters
            generated_data[column] = [''.join(random.choices(string.ascii_letters + string.digits + '\u4e00-\u9fa5', k=100)) for _ in range(batch_size)]
        elif 'varchar' in data_type.lower() and '32' in data_type.lower():
            # Generate unique random strings with a fixed length of 32 characters and a wide range of Unicode characters
            generated_data[column] = [''.join(random.choices(string.ascii_letters + string.digits + '\u4e00-\u9fa5' + '\U0001F600-\U0001F64F', k=32)) for _ in range(batch_size)]
        elif 'varchar' in data_type.lower() and '128' in data_type.lower():
            # Generate unique random strings with a fixed length of 128 characters and a wide range of Unicode characters
            generated_data[column] = [''.join(random.choices(string.ascii_letters + string.digits + '\u4e00-\u9fa5' + '\U0001F600-\U0001F64F', k=128)) for _ in range(batch_size)]
        elif 'varchar' in data_type.lower() and '90' in data_type.lower():
            # Generate more varied and randomized strings with a fixed length of 90 characters for VARCHAR(90)
            generated_data[column] = [''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + '\u4e00-\u9fa5', k=90)) for _ in range(batch_size)]
        elif 'float' in data_type.lower() and ('16' in data_type.lower() or '14' in data_type.lower()):
            # Generate a wider range of random floats for FLOAT(16,14)
            generated_data[column] = [round(random.uniform(-100000000000.00000, 100000000000.00000), 14) for _ in range(batch_size)]
        elif 'float' in data_type:
            # Generate a larger range of random floats
            generated_data[column] = [random.uniform(-1e6, 1e6) for _ in range(batch_size)]
        elif 'decimal' in data_type.lower():
            # Generate more varied and randomized decimal values for DECIMAL(4,4)
            generated_data[column] = [round(random.uniform(-100000.123456, 100000.123456), 6) for _ in range(batch_size)]
            # Adjust the range and precision as needed
        elif 'datetime' in data_type.lower():
            # Generate random datetime values spanning a wider range
            generated_data[column] = [fake.date_time_this_century(tzinfo=None, before_now=True, after_now=False) for _ in range(batch_size)]
        elif 'double' in data_type.lower():
            # Generate more varied and randomized doubles for DOUBLE
            generated_data[column] = [random.uniform(-1e10, 1e10) for _ in range(batch_size)]  # Adjust the range as needed
        elif 'mediumblob' in data_type.lower():
            generated_data[column] = [os.urandom(random.randint(2048, 4096)) for _ in range(batch_size)]  # Adjust the range of length as needed
        elif 'longblob' in data_type.lower():
            generated_data[column] = [os.urandom(random.randint(8192, 16384)) for _ in range(batch_size)]  # Adjust the range of length as needed
        elif 'varchar' in data_type.lower() and '30' in data_type.lower():
            # Generate more varied and randomized strings with a fixed length of 30 characters for VARCHAR(30)
            generated_data[column] = [''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + '\u4e00-\u9fa5', k=30)) for _ in range(batch_size)]
        elif 'varchar' in data_type.lower() and '54' in data_type.lower():
            # Generate more varied and randomized strings with a fixed length of 54 characters for VARCHAR(54)
            generated_data[column] = [
                ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + '\u4e00-\u9fa5', k=54))
                for _ in range(batch_size)
            ]
        elif 'varchar' in data_type.lower() and '512' in data_type.lower():
            # Generate more varied and randomized strings with a random length of 512 characters for VARCHAR(512)
            generated_data[column] = [
                ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + '\u4e00-\u9fa5', k=random.randint(201, 512)))
                for _ in range(batch_size)
            ]
        elif 'varchar' in data_type.lower() and '40' in data_type.lower():
            # Generate more varied and randomized strings with a fixed length of 40 characters for VARCHAR(40)
            generated_data[column] = [
                ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + '\u4e00-\u9fa5', k=random.randint(40)))
                for _ in range(batch_size)
            ]
        elif 'varchar' in data_type.lower() and '15' in data_type.lower():
            # Generate more varied and randomized strings with a random length of 15 characters for VARCHAR(15)
            generated_data[column] = [
                ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + '\u4e00-\u9fa5', k=random.randint(1, 15)))
                for _ in range(batch_size)
            ]
        elif 'varchar' in data_type.lower() and '100' in data_type.lower():
            # Generate more varied and randomized strings with a random length of 100 characters for VARCHAR(100)
            generated_data[column] = [
                ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + '\u4e00-\u9fa5', k=random.randint(10, 100)))
                for _ in range(batch_size)
            ]
        elif 'varchar' in data_type.lower() and '256' in data_type.lower():
            # Generate more varied and randomized strings with a fixed length of 256 characters for VARCHAR(256)
            generated_data[column] = [
                ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + '\u4e00-\u9fa5', k=256))
                for _ in range(batch_size)
            ]
        elif 'varchar' in data_type.lower() and '18' in data_type.lower():
            # Generate more varied and randomized strings with a fixed length of 18 characters for VARCHAR(18)
            generated_data[column] = [
                ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + '\u4e00-\u9fa5', k=random.randint(1, 18)))
                for _ in range(batch_size)
            ]
        elif 'varchar' in data_type.lower() and '1000' in data_type.lower():
            # Generate more varied and randomized strings with a random length of 1000 characters for VARCHAR(1000)
            generated_data[column] = [
                ''.join(random.choices(
                    string.ascii_letters + string.digits + string.punctuation + '\u4e00-\u9fa5' + '\U0001F600-\U0001F64F',  # Emojis
                    k=random.randint(600, 1000))
                )
                for _ in range(batch_size)
            ]
        elif 'varchar' in data_type.lower() and '200' in data_type.lower():
            # Generate random strings with a fixed length of 200 characters for VARCHAR(200)
            generated_data[column] = [''.join(random.choices(string.ascii_letters + string.digits + '\u4e00-\u9fa5' + '!@#$%^&*()_-+=', k=200)) for _ in range(batch_size)]
        elif 'varchar' in data_type.lower() and '64' in data_type.lower():
            # Generate unique random strings with a fixed length of 64 characters for VARCHAR(64)
            generated_data[column] = [''.join(random.choices(string.ascii_letters + string.digits + '\u4e00-\u9fa5' + '\U0001F600-\U0001F64F', k=64)) for _ in range(batch_size)]
        elif 'varchar' in data_type.lower() and '50' in data_type.lower():
            # Generate unique random strings with a fixed length of 50 characters for VARCHAR(50)
            generated_data[column] = [''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + '\u4e00-\u9fa5', k=50)) for _ in range(batch_size)]
        elif 'varchar' in data_type.lower() and '20' in data_type.lower():
            # Generate unique random strings with a fixed length of 20 characters for VARCHAR(20)
            generated_data[column] = [''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + '\u4e00-\u9fa5', k=20)) for _ in range(batch_size)]
        elif 'varchar' in data_type.lower() and '10' in data_type.lower():
            # Generate unique random strings with a fixed length of 10 characters for VARCHAR(10)
            generated_data[column] = [''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + '\u4e00-\u9fa5', k=10)) for _ in range(batch_size)]
        elif 'varchar' in data_type.lower() and '8' in data_type.lower():
            # Generate unique random strings with a fixed length of 8 characters for VARCHAR(8)
            generated_data[column] = [''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + '\u4e00-\u9fa5', k=8)) for _ in range(batch_size)]
        elif 'timestamp' in data_type.lower():
            # Generate random datetime values spanning a wider range for TIMESTAMP
            generated_data[column] = [fake.date_time_this_decade(tzinfo=None, before_now=True, after_now=False) for _ in range(batch_size)]
        elif 'blob' in data_type.lower():
            # Generate more randomized binary data for BLOB
            generated_data[column] = [os.urandom(random.randint(1024, 2048)) for _ in range(batch_size)]  # Adjust the range of length as needed
        elif 'varchar' in data_type.lower():
            # Generate more varied and randomized strings with varying lengths for VARCHAR
            generated_data[column] = [
                ''.join(random.choices(
                    string.ascii_letters + string.digits + string.punctuation + string.whitespace + '\u4e00-\u9fa5' + '\U0001F600-\U0001F64F',  # Include Unicode characters and emojis
                    k=random.randint(10, 255))  # Adjust the maximum length as needed
                )
                for _ in range(batch_size)
            ]
        elif 'json' in data_type.lower():
            # Generate more complex and larger JSON structures for JSON
            generated_data[column] = [
                {
                    'key1': fake.word(),
                    'key2': fake.random_number(),
                    'nested_key': {'nested_key1': fake.word(), 'nested_key2': fake.random_number()},
                    'large_data': [{'item': fake.word()} for _ in range(100)]  # Example: 100 items in the array
                }
                for _ in range(batch_size)
            ]
        # Add more conditions for other data types as needed


    return generated_data

# Read column data types from RDS table
query = f"SHOW COLUMNS FROM {rds_table_name}"
df_columns = pd.read_sql(query, con=engine)
column_data_types = dict(zip(df_columns['Field'], df_columns['Type']))

# Foreign key and dependent columns 
foreign_keys = foreign_and_dep_keys

# Batch size for generating and inserting data
batch_size = 1  # Example: 5000 rows per batch

# Number of rows to generate (adjust as needed)
num_rows = 2  # Example: generate 500,000 rows

config = readJson("./config.json")
# Generate and insert data in batches
for i in range(0, num_rows, batch_size):
    # Generate fake data based on column data types, excluding auto-incremented primary keys
    generated_data = generate_fake_data(column_data_types, foreign_keys, batch_size, config)

    # Convert the generated data to a DataFrame
    df_data = pd.DataFrame(generated_data)

    # Insert data into the RDS table (append to existing data)
    df_data.to_sql(name=rds_table_name, con=engine, if_exists='append', index=False)

    print("Batch completed %s", str(i))

print("Sample data appended to %s based on column data types and provided foreign key values.", str(rds_table_name))
