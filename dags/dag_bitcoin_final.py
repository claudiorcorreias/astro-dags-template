# Importando as bibliotecas necessárias
import pendulum
import requests
import pandas as pd
from airflow.decorators import dag, task
from google.cloud import bigquery
from pandas_gbq import to_gbq

# --------------------------------------------------------------------------
# CONFIGURAÇÃO - PREENCHA COM AS SUAS INFORMAÇÕES
# --------------------------------------------------------------------------
PROJECT_ID = 'exercicio-etl-bitcoin'  # Substitua pelo ID do seu projeto no GCP
DATASET_NAME = 'bitcoin_data'  # Substitua pelo nome do seu dataset no BigQuery
TABLE_NAME = 'bitcoin_history_claudio' # Nome da tabela que será criada ou substituída
# --------------------------------------------------------------------------





@dag(
    dag_id='exercicio_9_1_bitcoin_bq_final',
    schedule=None,  # Defina como None para execuções manuais ou '@daily' para diárias
    start_date=pendulum.datetime(2025, 9, 27, tz="UTC"),
    catchup=False,
    tags=['exercicio', 'bitcoin', 'bigquery'],
    description="DAG para buscar o histórico de 6 meses do Bitcoin e carregar no BigQuery."
)
def bitcoin_to_bigquery_dag():
    """
    ### DAG de Extração e Carga de Dados do Bitcoin

    Esta DAG realiza as seguintes tarefas:
    1.  **Extrai** dados históricos de preços do Bitcoin dos últimos 6 meses da API da CoinGecko.
    2.  **Transforma** os dados usando a biblioteca Pandas.
    3.  **Carrega** os dados resultantes em uma tabela no Google BigQuery.
    """
    @task
    def fetch_and_load_bitcoin_data_to_bq():
        """
        Busca os dados da API da CoinGecko e os carrega no BigQuery.
        """
        print("Iniciando a busca de dados na API da CoinGecko...")
        
        # URL e parâmetros para a API
        url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
        params = {
            "vs_currency": "usd",
            "days": "180",  # Últimos 6 meses
            "interval": "daily"
        }
        
        # Fazendo a requisição para a API
        response = requests.get(url, params=params)
        response.raise_for_status()  # Isso irá gerar um erro se a requisição falhar
        data = response.json()
        prices = data["prices"]
        
        print(f"{len(prices)} registros de preço diário foram encontrados.")

        # Verificando se a lista de preços não está vazia
        if not prices:
            raise ValueError("A API não retornou dados de preço.")

        # Transformando os dados em um DataFrame do Pandas
        df = pd.DataFrame(prices, columns=["timestamp_ms", "price_usd"])
        
        # Convertendo o timestamp para um formato de data legível e ajustando as colunas
        df['price_date'] = pd.to_datetime(df['timestamp_ms'], unit='ms').dt.date
        df = df[['price_date', 'price_usd']]
        
        print("Dados transformados com sucesso. Exemplo:")
        print(df.head())

        # Carregando o DataFrame para o BigQuery
        destination_table = f"{DATASET_NAME}.{TABLE_NAME}"
        print(f"Carregando dados para a tabela: {PROJECT_ID}.{destination_table}")

        to_gbq(
            dataframe=df,
            destination_table=destination_table,
            project_id=PROJECT_ID,
            if_exists='replace'  # 'replace': apaga a tabela e a recria. 
                                 # 'append': adiciona os dados se a tabela já existir.
        )
        
        print("Carga de dados no BigQuery concluída com sucesso!")

    fetch_and_load_bitcoin_data_to_bq()

# Instanciando a DAG para que o Airflow a reconheça
bitcoin_to_bigquery_dag()