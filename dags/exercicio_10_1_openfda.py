# Importa as bibliotecas necessárias. "annotations" ajuda na digitação de código futuro.
from __future__ import annotations

# Ferramentas do Airflow para criar DAGs e Tasks.
from airflow.decorators import dag, task

# O "Hook" é a ferramenta específica do Airflow para se conectar e interagir com o BigQuery.
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# "pendulum" é usado para lidar com datas e horas de forma mais fácil e robusta.
import pendulum

# "pandas" é a biblioteca principal para manipulação de dados em formato de tabela (DataFrame).
import pandas as pd

# "requests" é usado para fazer chamadas à API da OpenFDA pela internet.
import requests

# "datetime" é a biblioteca padrão do Python para trabalhar com datas.
from datetime import date

# --- INÍCIO DO BLOCO DE CONFIGURAÇÕES ---
# Nesta seção, definimos todas as variáveis que controlam o comportamento da nossa DAG.

# [OBRIGATÓRIO] Coloque aqui o ID do seu projeto no Google Cloud.
GCP_PROJECT  = "exercicio-etl-bitcoin"

# [CORRETO] Nome do dataset que você já criou no BigQuery.
BQ_DATASET   = "dataset_fda"

# [PODE MANTER] Nome da tabela que o script criará automaticamente no BigQuery.
BQ_TABLE     = "openfda_events_daily"

# [ATENÇÃO!] Verifique no BigQuery qual é a "Localização dos dados" do seu dataset e use o mesmo valor aqui.
# Exemplos comuns: "US", "us-central1", "southamerica-east1". Errar isso causará falha.
BQ_LOCATION  = "US"

# [PODE MANTER] Este é o ID padrão da conexão com o Google Cloud que configuraremos na interface do Airflow.
GCP_CONN_ID  = "google_cloud_default"

# [PODE MANTER] Nome do Pool que criaremos para limitar as chamadas à API. É uma boa prática.
POOL_NAME    = "openfda_api"

# [PODE ALTERAR] Este é o termo de busca na API. Você pode trocar 'sildenafil+citrate' por outro medicamento.
# O "+" representa um espaço na URL.
DRUG_QUERY = 'sildenafil+citrate'

# --- FIM DO BLOCO DE CONFIGURAÇÕES ---

# Cria uma sessão de requests. Isso permite reutilizar a mesma conexão para várias chamadas, o que é mais eficiente.
# O "User-Agent" ajuda a identificar seu script para a API, o que é uma boa prática.
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "claudiocorreias-etl/1.0"})

def _openfda_get(url: str) -> dict:
    """Função auxiliar para fazer a chamada GET à API e tratar respostas comuns."""
    print(f"Consultando URL: {url}")
    r = SESSION.get(url, timeout=30)
    # Se a API retornar 404, significa "Não encontrado". Para nós, isso quer dizer "sem resultados", o que não é um erro.
    if r.status_code == 404:
        return {"results": []}
    # Se houver qualquer outro erro (como 500, 403, etc.), esta linha irá parar o script e marcar a task como falha.
    r.raise_for_status()
    # Se a chamada for bem-sucedida, retorna os dados em formato JSON (um dicionário Python).
    return r.json()

def _build_openfda_url(start: date, end: date, drug_query: str) -> str:
    """Função auxiliar para montar a URL da API dinamicamente com base nas datas e no medicamento."""
    start_str = start.strftime("%Y%m%d")
    end_str   = end.strftime("%Y%m%d")
    return (f"https://api.fda.gov/drug/event.json"
            f"?search=patient.drug.medicinalproduct:%22{drug_query}%22"
            f"+AND+receivedate:[{start_str}+TO+{end_str}]"
            f"&count=receivedate")

# O decorador `@task` transforma esta função Python em uma Task do Airflow.
# O `pool=POOL_NAME` garante que esta task só execute se houver um "slot" disponível no pool "openfda_api".
@task(pool=POOL_NAME)
def fetch_range_and_to_bq(start_date: str, end_date: str):
    """
    Esta é a task principal. Ela executa o processo completo:
    1. Monta a URL da API.
    2. Busca os dados.
    3. Transforma os dados usando Pandas.
    4. Carrega os dados no BigQuery.
    """
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    
    # 1. Monta a URL
    url = _build_openfda_url(start, end, DRUG_QUERY)
    
    # 2. Busca os dados
    data = _openfda_get(url)
    results = data.get("results", [])
    
    if not results:
        print(f"Nenhum resultado encontrado para o período: {start} a {end}. Encerrando a task com sucesso.")
        return # Termina a função aqui se não houver dados.
    
    # 3. Transforma os dados
    # Converte a lista de resultados em um DataFrame do Pandas e renomeia a coluna "count" para "events".
    df = pd.DataFrame(results).rename(columns={"count": "events"})
    # Converte a coluna "time" (que vem como string "YYYYMMDD") para um formato de data e hora adequado.
    df["time"] = pd.to_datetime(df["time"], format="%Y%m%d", utc=True)
    # Adiciona uma nova coluna com o nome do medicamento pesquisado.
    df["drug"] = DRUG_QUERY.replace("+", " ")
    
    # 4. Carrega no BigQuery
    print(f"Carregando {len(df)} registros no BigQuery na tabela {BQ_DATASET}.{BQ_TABLE}...")
    # Cria uma instância do BigQueryHook, que sabe como se conectar ao BigQuery usando a conexão que criamos.
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION, use_legacy_sql=False)
    
    # O método `to_gbq` do Pandas é uma forma muito conveniente de enviar um DataFrame para o BigQuery.
    df.to_gbq(
        destination_table=f"{BQ_DATASET}.{BQ_TABLE}", # Define a tabela de destino.
        project_id=GCP_PROJECT, # Especifica o projeto.
        if_exists="append", # "append" significa que os novos dados serão adicionados ao final da tabela se ela já existir.
        credentials=bq_hook.get_credentials(), # Usa as credenciais obtidas através do hook e da conexão do Airflow.
        # `table_schema` define a estrutura da tabela. É importante para garantir a consistência dos tipos de dados.
        table_schema=[
            {"name": "time", "type": "TIMESTAMP"},
            {"name": "events", "type": "INTEGER"},
            {"name": "drug", "type": "STRING"}
        ],
        location=BQ_LOCATION,
        progress_bar=False
    )
    print("Carga de dados no BigQuery concluída com sucesso.")

# O decorador `@dag` transforma a função abaixo em uma DAG.
@dag(
    dag_id="openfda_etl_para_bigquery_exercicio", # Nome único da DAG na interface do Airflow.
    schedule=None, # `None` significa que a DAG só rodará quando acionada manualmente (não tem agendamento).
    start_date=pendulum.datetime(2025, 9, 25, tz="UTC"), # Data de início. Boa prática usar uma data no passado.
    catchup=False, # Impede que a DAG execute para datas passadas que ela "perdeu".
    tags=["openfda", "bigquery", "exercicio_10_1"] # Etiquetas para facilitar a busca na UI.
)
def openfda_pipeline():
    """Esta função define a estrutura da DAG, ou seja, quais tasks ela contém e em que ordem."""
    # Para este exercício, chamamos a task com um intervalo de datas fixo.
    fetch_range_and_to_bq(start_date="2025-07-01", end_date="2025-07-31")

# Esta linha final instancia a DAG, tornando-a visível para o Airflow.
dag = openfda_pipeline()