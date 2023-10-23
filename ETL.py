import sqlite3
import csv

csv_file = 'data.csv'
db_file = 'banco.db'

conn = sqlite3.connect(db_file)
cursor = conn.cursor()

drop_table_query = 'DROP TABLE IF EXISTS data'
cursor.execute(drop_table_query)

with open(csv_file, 'r') as file:
    csv_reader = csv.DictReader(file)
    sorted_data = sorted(csv_reader, key=lambda row: int(row['Year']))

header = sorted_data[0].keys()
header_with_percent_diff = list(header) + ["PercentDifference"]
columns = ', '.join([f'"{col}" TEXT' for col in header_with_percent_diff])
create_table_query = f'CREATE TABLE data ({columns})'
cursor.execute(create_table_query)

prev_life_expectancy = None

for row in sorted_data:
    if row['Country'] == 'Brazil':
        current_life_expectancy = float(row['Life expectancy '])
        if prev_life_expectancy is not None:
            percent_difference = (current_life_expectancy - prev_life_expectancy) / prev_life_expectancy * 100
        else:
            percent_difference = None

        values = [row[column] for column in header] + [percent_difference]
        placeholders = ', '.join(['?'] * len(values))
        insert_query = f'INSERT INTO data VALUES ({placeholders})'
        cursor.execute(insert_query, values)

        prev_life_expectancy = current_life_expectancy

conn.commit()

query = 'SELECT * FROM data'
cursor.execute(query)
new_data = cursor.fetchall()

for row in new_data:
    print(row)

conn.close()
