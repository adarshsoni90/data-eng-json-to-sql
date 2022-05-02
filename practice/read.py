import os


def get_json_reader(BASE_DIR, DIR_NAME, table_name, chunk_size=1000):
    import pandas as pd
    import os.path
    fp = f'{BASE_DIR}/{DIR_NAME}/{table_name}.json'
    print('file', fp)
    if os.path.exists(fp):
        print('Found')
    else: print('not found')
    return pd.read_json(fp, lines= True, chunksize=chunk_size)


if __name__ == '__main__':
    BASE_DIR = os.environ.get('BASE_DIR')
    table_name = os.environ.get('TABLE_NAME')
    file_name = input('Enter the file name: ')
    json_reader = get_json_reader(BASE_DIR, table_name, table_name)
    print("got file")
    for idx, df in enumerate(json_reader):
        print(f'number of records in chunk with index {idx} is {df.shape[0]}')
