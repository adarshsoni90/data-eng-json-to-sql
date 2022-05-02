import os
from read import get_json_reader
from write import load_db_table
import sqlalchemy as db


def process_tables(BASE_DIR, DIR_NAME, conn, table_name):
    json_reader = get_json_reader(BASE_DIR, DIR_NAME, table_name)
    for df in json_reader:
        load_db_table(df, conn, table_name, df.columns[0])


def main():
    BASE_DIR = os.environ.get('BASE_DIR')
    DIR_NAME = os.environ.get('DIR_NAME')
    table_names = input('Enter the table name: ')
    engine = db.create_engine("mysql+pymysql://Adarsh:Adarsh75@localhost/sakila")
    conn = engine.connect()
    for table_name in table_names.split(','):
        process_tables(BASE_DIR, DIR_NAME, conn, table_name)


if __name__ == '__main__':
    main()
