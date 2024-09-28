import sys
import csv
import sqlite3
import re


def main():
    args = sys.argv[1:]
    filename = args[0]
    if filename == '-h' or filename == '--help':
        print('Usage:')
        print('python csvsql.py myDataFile.csv \';\'')
        print('where ; is the delimiter')
        quit()
    delimiter = ',' if len(args) < 2 else args[1]
    with open(filename, newline='', encoding='utf-8') as csvfile:
        datareader = csv.DictReader(csvfile, delimiter=delimiter)
        headers = datareader.fieldnames
        headers = [re.sub(r'\W', '', x) for x in headers]
        print('Read dataset with columns: ')
        print(', '.join(headers))
        connection = sqlite3.connect("dataset.db")
        cursor = connection.cursor()
        try:
            res = cursor.execute('SELECT name FROM sqlite_master where name="data"')
            if res.fetchone() is not None:
                cursor.execute('DROP TABLE data')
            create_statement = 'CREATE TABLE data(' + ','.join(headers) + ')'
            cursor.execute(create_statement)
            res = cursor.execute('SELECT name FROM sqlite_master')
            if res.fetchone() is None:
                print('Table creation failed')
                quit()
            print('Table "data" created')

            values = [tuple(row.values()) for row in datareader]
            placeholders = ','.join(['?' for _ in headers])
            cursor.executemany('INSERT INTO data VALUES(' + placeholders + ')', values)
            connection.commit()
            print(f'Inserted {len(values)} entries')

            while True:
                print('Enter query')
                query = input()
                if query == 'quit':
                    quit()
                try:
                    res = cursor.execute(query).fetchall()
                    print(f'Found {len(res)} entries')
                    for entry in res:
                        print(entry)
                except Exception as e:
                    print(f'An error occurred: {e}')
        except Exception as e:
            print(f'An error occurred: {e}')
        finally:
            res = cursor.execute('SELECT name FROM sqlite_master where name="data"')
            if res is not None:
                cursor.execute('DROP TABLE data')
            connection.close()


if __name__ == "__main__":
    main()