import json
import time
import pandas as pd
import psycopg2

BATCH_SIZE = 1000
NUMBER_OF_ROWS = 50001
# Database connection parameters
db_params = {
    'dbname': 'dbname',
    'user': 'user',
    'password': 'password',
    'host': 'localhost',
    'port': '5432'
}

original_json_file_path = "/path/to/tos.product_texts.json"
# Load the JSON data
t0 = time.perf_counter()
with open(original_json_file_path, 'r', encoding='utf-8') as file:
    data = json.load(file)
t1 = time.perf_counter()
count = 1
print(f"BATCH SIZE : {BATCH_SIZE}")

for step in range(0, NUMBER_OF_ROWS, BATCH_SIZE):
    print(f"PROCESSING BATCH #{count}")
    iteration_starts = time.perf_counter()
    # Convert JSON data to DataFrame
    flattened_list = pd.DataFrame(data[step: step + BATCH_SIZE]).apply(
        lambda row: "\n".join([
                                  f'''INSERT INTO products (oid, text, language, translation) VALUES('{row["_id"]["$oid"]}', '{row["text"].replace("'", "''")}', '{key}', '{row["translations"][key].replace("'", "''")}');'''
                                  for key in row['translations'].keys()]), axis=1).str.cat(sep=' ')
    try:
        # Execute the create table query
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        cursor.execute(flattened_list)
        connection.commit()
        cursor.close()
        connection.close()
    except Exception as error:
        print(f"Error while creating table: {error}")
        cursor.close()
        connection.close()

    del flattened_list
    print(f"BATCH #{count} took {time.perf_counter() - iteration_starts} secs")
    count += 1

print("TOOK ", t1 - t0, " SECONDS TO LOAD FILE & ", time.perf_counter() - t1, " SECONDS PROCESSING")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
