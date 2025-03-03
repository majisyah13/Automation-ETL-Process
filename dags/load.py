from pymongo import MongoClient
import pandas as pd

path = '/opt/airflow/dags'

def load(data):
    client = MongoClient('mongodb+srv://admin:admin@tugas.ifs8i.mongodb.net/')
    db = client['M3']
    collection = db['airflow']

    data_dict = data.to_dict(orient='records')

    if data_dict:
        collection.insert_many(data_dict)
    else:
        print('No data to insert.')

    client.close()

if __name__== "__main__":
    df = pd.read_csv('/opt/airflow/logs/data_staging/data_cleaned.csv')
    load(df)