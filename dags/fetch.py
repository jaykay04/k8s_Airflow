from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import json

def get_data(**kwargs):
    url = "https://raw.githubusercontent.com/airscholar/ApacheFlink-SalesAnalytics/main/out/new-output.csv"
    response = requests.get(url)

    if response.status_code == 200:
        df = pd.read_csv(url, header=None, names=["category", "price", "quantity"])

        #convert dataframe to json strings from xcomm
        json_data = df.to_json(orient="records")

        kwargs['ti'].xcom_push(key='data', value=json_data)
    else:
        raise Exception(f"Failed to get data, HTTP response code: {response.status_code}")


def preview_data(**kwargs):
    output_data = kwargs['ti'].xcom_pull(key='data', task_ids='get_data')
    print(output_data)
    if output_data:
        output_data = json.loads(output_data)
    else:
        raise ValueError('No data recieved from XComm')

    # Create dataframe from the JSON data
    df = pd.DataFrame(output_data)

    # Compute total sales
    df['total_sales'] = df['price'] * df['quantity']
    df = df.groupby('category', as_index=False).agg({'quantity': 'sum', 'total_sales': 'sum'})

    # Sort by total_sales
    df = df.sort_values('total_sales', ascending=False)
    print(df[['category', 'total_sales']]).head(20)

default_args = {
    "owner": "jaykay",
    "start_date": datetime(2024, 1, 25),
    "catchup": False
}

dag = DAG(
    dag_id = "fetch_data",
    default_args = default_args,
    schedule = timedelta(days=1)
)

get_data_from_url = PythonOperator(
    task_id = "get_data",
    python_callable = get_data,
    dag = dag
)

preview_data_from_url = PythonOperator(
    task_id = "preview_data",
    python_callable=preview_data,
    dag = dag
)

get_data_from_url >> preview_data_from_url