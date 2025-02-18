"""
Bitcoin_price2
DAG auto-generated by Astro Cloud IDE.
"""

from airflow.decorators import dag
from astro import sql as aql
import pandas as pd
import pendulum


@aql.dataframe(task_id="python_1")
def python_1_func():
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import datetime, timedelta
    import pandas as pd
    import requests
    
    # Função para coletar dados do Bitcoin
    def get_bitcoin_data():
        url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
        params = {
            "vs_currency": "usd",
            "days": "180",
            "interval": "daily"
        }
        response = requests.get(url, params=params)
        data = response.json()
        prices = data["prices"]
        
        df = pd.DataFrame(prices, columns=["timestamp", "price"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df.set_index("timestamp", inplace=True)
        
        # Salva os dados em um arquivo CSV
        df.to_csv("bitcoin_prices.csv")
    
    # Configuração da DAG
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2023, 1, 1),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
    
    dag = DAG(
        "bitcoin_price_dashboard",
        default_args=default_args,
        description="DAG para coletar dados do Bitcoin e gerar dashboard",
        schedule="@daily",  # Executa diariamente
    )
    
    # Tarefa para coletar dados
    fetch_data_task = PythonOperator(
        task_id="fetch_bitcoin_data",
        python_callable=get_bitcoin_data,
        dag=dag,
    )
    
    # Tarefa para gerar dashboard (substitua por sua lógica)
    def generate_dashboard():
        # Aqui você pode integrar com o Looker Studio ou outra ferramenta
        print("Dashboard gerado com sucesso!")
    
    generate_dashboard_task = PythonOperator(
        task_id="generate_dashboard",
        python_callable=generate_dashboard,
        dag=dag,
    )
    
    # Define a ordem das tarefas
    fetch_data_task >> generate_dashboard_task

default_args={
    "owner": "Claudio Correia,Open in Cloud IDE",
}

@dag(
    default_args=default_args,
    schedule="0 0 * * *",
    start_date=pendulum.from_format("2025-02-07", "YYYY-MM-DD").in_tz("UTC"),
    catchup=False,
    owner_links={
        "Claudio Correia": "mailto:claudiorcorreias@gmail.com",
        "Open in Cloud IDE": "https://cloud.astronomer.io/cm6ik17yd02j301l01w52s1xu/cloud-ide/cm6m7quz50scy01mdcf2fh09r/cm6uv1vw406mf01lahe6ixbte",
    },
)
def Bitcoin_price2():
    python_1 = python_1_func()

dag_obj = Bitcoin_price2()
