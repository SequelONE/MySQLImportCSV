import os
import csv
import mysql.connector
from transliterate import translit
import re
import hashlib

# MySQL connection parameters
cnx = mysql.connector.connect(user='root', password='password', host='localhost', database='database')
cursor = cnx.cursor()

# Directory containing CSV files
csv_directory = 'csv'

# Specify the table name manually
table_name = 'articles'  # Replace with your desired table name

# Function to check if a table exists
def table_exists(table_name):
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = DATABASE() AND table_name = %s
        """, (table_name,))
    return cursor.fetchone()[0] == 1

# Function to check if a column exists in a table
def column_exists(table_name, column_name):
    cursor.execute("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = %s AND column_name = %s
        """, (table_name, column_name))
    return cursor.fetchone()[0] == 1

# Function to transliterate Russian field names to English
def transliterate_field_names(field_names):
    transliterated = []
    for name in field_names:
        # Remove any leading/trailing whitespace
        name = name.strip()
        if not name:
            continue  # Skip empty field names
        # Transliterate from Russian to English
        transliterated_name = translit(name, 'ru', reversed=True)
        # Replace non-word characters with underscores
        transliterated_name = re.sub(r'\W', '_', transliterated_name)
        transliterated_name = re.sub(r'_+', '_', transliterated_name)  # Replace multiple underscores with one
        transliterated_name = transliterated_name.strip('_')  # Remove leading/trailing underscores
        if not transliterated_name:
            continue  # Skip if the name is empty after cleaning
        transliterated.append(transliterated_name)
    return transliterated

# Ensure the table exists and has an 'id' primary key and 'record_hash' column
if not table_exists(table_name):
    create_table_query = f"""
    CREATE TABLE `{table_name}` (
        `id` INT AUTO_INCREMENT PRIMARY KEY,
        `record_hash` VARCHAR(64) UNIQUE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """
    cursor.execute(create_table_query)
    print(f"Table '{table_name}' created with primary key 'id' and unique 'record_hash' column.")
else:
    # Check if 'record_hash' column exists, if not, add it
    if not column_exists(table_name, 'record_hash'):
        alter_table_query = f"ALTER TABLE `{table_name}` ADD COLUMN `record_hash` VARCHAR(64) UNIQUE"
        cursor.execute(alter_table_query)
        print(f"Column 'record_hash' added to table '{table_name}'.")
    print(f"Table '{table_name}' already exists.")

# Process each CSV file in the directory
for filename in os.listdir(csv_directory):
    if filename.endswith('.csv'):
        csv_file_path = os.path.join(csv_directory, filename)
        print(f"\nProcessing file: {filename}")

        try:
            with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
                # Since your CSV files use ';' as delimiter, specify it explicitly
                reader = csv.reader(csvfile, delimiter=';', quotechar='"')
                # Read the header row and transliterate field names
                original_field_names = next(reader)
                if not original_field_names:
                    print(f"Skipping file '{filename}' because the header row is empty.")
                    continue
                print(f"Original field names: {original_field_names}")
                field_names = transliterate_field_names(original_field_names)
                if not field_names:
                    print(f"Skipping file '{filename}' because after transliteration, no valid field names were found.")
                    continue
                print(f"Transliterated field names: {field_names}")

                # Add new columns to the table if they don't exist
                for col in field_names:
                    if not column_exists(table_name, col):
                        alter_table_query = f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` TEXT"
                        cursor.execute(alter_table_query)
                        print(f"Column '{col}' added to table '{table_name}'.")

                # Prepare the insert query (excluding 'id' and 'record_hash')
                placeholders = ', '.join(['%s'] * len(field_names))
                columns = ', '.join(f"`{col}`" for col in field_names)
                insert_query = f"INSERT INTO `{table_name}` ({columns}, `record_hash`) VALUES ({placeholders}, %s)"

                # Prepare a set to store existing record hashes for quick lookup
                cursor.execute(f"SELECT `record_hash` FROM `{table_name}`")
                existing_hashes = set(row[0] for row in cursor.fetchall())

                # Insert data rows
                inserted_rows = 0
                for row in reader:
                    # Adjust the row length to match the number of columns
                    row_data = row[:len(field_names)]
                    if len(row_data) < len(field_names):
                        row_data.extend([None] * (len(field_names) - len(row_data)))  # Pad with None

                    # Compute hash of the row data
                    row_string = ''.join(str(item) if item is not None else '' for item in row_data)
                    record_hash = hashlib.sha256(row_string.encode('utf-8')).hexdigest()

                    # Skip if the record already exists
                    if record_hash in existing_hashes:
                        continue

                    # Add the hash to the existing hashes set
                    existing_hashes.add(record_hash)

                    # Insert the row with the record_hash
                    cursor.execute(insert_query, (*row_data, record_hash))
                    inserted_rows += 1

                cnx.commit()
                print(f"Inserted {inserted_rows} new records from '{filename}' into table '{table_name}'.")

        except Exception as e:
            print(f"An error occurred while processing file '{filename}': {e}")

# Close the cursor and connection
cursor.close()
cnx.close()
