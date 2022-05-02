def load_db_table(df, conn, table_name, key):
    min_key = df[key].min()
    max_key = df[key].max()
    df.to_sql(table_name, conn, if_exists='append', index= False)
    print(f'loaded data for {table_name} within the range of {min_key} and {max_key}')
    if int(max_key)%1000 != 0:
        print(f'loading table {table_name} successful')


if __name__ == '__main__':
    import pandas as pd
    import sqlalchemy as db
    import os

    data = [
        {'user_id': 1, 'user_first_name': 'Scott', 'user_last_name': 'Tiger'},
        {'user_id': 2, 'user_first_name': 'Donald', 'user_last_name': 'Duck'}
    ]
    df = pd.DataFrame(data)
    configs = dict(os.environ.items())
    engine = db.create_engine("mysql+pymysql://Adarsh:Adarsh75@localhost/sakila")
    conn = engine.connect()
    load_db_table(df, conn, 'users', 'user_id')
