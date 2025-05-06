# Importações de bibliotecas
import polars as pl  # Biblioteca para manipulação eficiente de dados em DataFrames
from datetime import datetime
import pytz  # Trabalhar com fusos horários
from opendotaapi import OpenDotaAPI  # Cliente personalizado para acessar a API do OpenDota
import os  # Acesso a variáveis de ambiente e manipulação de arquivos
import boto3  # Cliente AWS para interagir com serviços como S3
import pyarrow as pa  # Conversão e manipulação de dados em memória
import pyarrow.parquet as pq  # Escrita de dados no formato Parquet
from botocore.exceptions import ClientError
import json  # Trabalhar com dados no formato JSON
import glob  # Localizar arquivos locais com padrões

def setup_aws_credentials():
    """
    Obtém as credenciais AWS definidas como variáveis de ambiente 
    (por exemplo, via GitHub Actions) e configura o cliente S3.
    """
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    aws_region = os.environ.get('AWS_REGION', 'us-east-1')  # Região padrão

    if not aws_access_key or not aws_secret_key:
        raise ValueError("Credenciais AWS não encontradas nos segredos do GitHub Actions")

    # Retorna um cliente autenticado para o S3
    return boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )

def clean_local_files():
    """
    Limpa os arquivos locais criados durante a execução (formato parquet, JSON ou CSV).
    Usado para evitar acúmulo de arquivos temporários.
    """
    try:
        extensions = ['*.parquet', '*.json', '*.csv']  # Tipos de arquivos a remover

        for ext in extensions:
            files = glob.glob(ext)  # Busca arquivos com a extensão
            for file in files:
                try:
                    os.remove(file)
                    print(f"Arquivo local {file} deletado com sucesso")
                except Exception as e:
                    print(f"Erro ao deletar arquivo {file}: {str(e)}")

        print("Limpeza de arquivos locais concluída")
    except Exception as e:
        print(f"Erro durante a limpeza de arquivos: {str(e)}")

def save_to_s3(df, s3_client, bucket, key):
    """
    Salva um DataFrame Polars no Amazon S3 como arquivo Parquet usando PyArrow.
    
    Parâmetros:
    - df: DataFrame Polars
    - s3_client: cliente boto3 do S3
    - bucket: nome do bucket
    - key: caminho do arquivo no bucket
    """
    try:
        table = df.to_arrow()  # Converte de Polars para Arrow
        buffer = pa.BufferOutputStream()  # Cria buffer em memória

        pq.write_table(table, buffer)  # Escreve como parquet no buffer
        buffer_content = buffer.getvalue().to_pybytes()  # Extrai conteúdo

        # Envia o conteúdo ao S3
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer_content
        )

        print(f"Arquivo salvo com sucesso: s3://{bucket}/{key}")

    except Exception as e:
        print(f"Erro ao salvar arquivo {key}: {str(e)}")
        raise

def create_dataframe_from_json(data, infer_schema_length=None):
    """
    Cria um DataFrame Polars a partir de dados JSON, com suporte a diferentes formatos.
    
    Parâmetros:
    - data: pode ser uma lista de dicionários, um dicionário único ou string JSON
    - infer_schema_length: número de linhas usadas para inferir o schema
    """
    try:
        if isinstance(data, list) and all(isinstance(item, dict) for item in data):
            return pl.DataFrame(data, infer_schema_length=infer_schema_length)

        if isinstance(data, str):
            data = json.loads(data)

        if isinstance(data, dict):
            data = [data]

        return pl.DataFrame(data, infer_schema_length=infer_schema_length)

    except Exception as e:
        print(f"Erro ao criar DataFrame: {str(e)}")
        print(f"Tipo dos dados: {type(data)}")
        if isinstance(data, list) and len(data) > 0:
            print(f"Exemplo do primeiro item: {data[0]}")
        raise

def extract_all_data():
    """
    Função principal que orquestra a extração dos dados da API OpenDota,
    transforma em DataFrames e salva no S3.
    """
    s3_client = setup_aws_credentials()  # Autentica com a AWS
    api = OpenDotaAPI()  # Inicializa a API do OpenDota

    # Caminho base no S3 onde os dados serão salvos
    bucket = "scarstimeslake"
    base_path = "dota/stage/api/full-load"

    print(f"Iniciando extração de dados do Dota 2...")

    try:
        # Extração de entidades básicas da API
        print("\nExtraindo dados básicos...")
        matches_df = api.get_matches()
        if matches_df is not None:
            save_to_s3(matches_df, s3_client, bucket, f"{base_path}/matches.parquet")

        heroes_df = api.get_heroes()
        if heroes_df is not None:
            save_to_s3(heroes_df, s3_client, bucket, f"{base_path}/heroes.parquet")

        lobby_types_df = api.get_lobby_types()
        if lobby_types_df is not None:
            save_to_s3(lobby_types_df, s3_client, bucket, f"{base_path}/lobby_types.parquet")

        game_modes_df = api.get_game_modes()
        if game_modes_df is not None:
            save_to_s3(game_modes_df, s3_client, bucket, f"{base_path}/game_modes.parquet")

        clusters_df = api.get_clusters()
        if clusters_df is not None:
            save_to_s3(clusters_df, s3_client, bucket, f"{base_path}/clusters.parquet")

        # Extração de dados relacionados a times
        print("\nExtraindo dados de times...")
        teams_data = api.get_teams()
        if teams_data:
            teams_df = create_dataframe_from_json(teams_data, infer_schema_length=1000)
            save_to_s3(teams_df, s3_client, bucket, f"{base_path}/teams.parquet")

        # Extração de dados de ligas
        print("\nExtraindo dados de ligas...")
        leagues_data = api.get_leagues()
        if leagues_data:
            leagues_df = create_dataframe_from_json(leagues_data, infer_schema_length=1000)
            save_to_s3(leagues_df, s3_client, bucket, f"{base_path}/leagues.parquet")

        # Extração de jogadores profissionais
        print("\nExtraindo dados de jogadores profissionais...")
        pro_players_data = api.get_pro_players()
        if pro_players_data:
            pro_players_df = create_dataframe_from_json(pro_players_data, infer_schema_length=1000)
            save_to_s3(pro_players_df, s3_client, bucket, f"{base_path}/pro_players.parquet")

        # Extração de partidas profissionais
        print("\nExtraindo dados de partidas profissionais...")
        pro_matches_data = api.get_pro_matches()
        if pro_matches_data:
            pro_matches_df = create_dataframe_from_json(pro_matches_data, infer_schema_length=1000)
            save_to_s3(pro_matches_df, s3_client, bucket, f"{base_path}/pro_matches.parquet")

        # Extração de rankings de heróis
        print("\nExtraindo rankings de heróis...")
        hero_rankings_data = api.get_hero_rankings()
        if hero_rankings_data:
            hero_rankings_df = create_dataframe_from_json(hero_rankings_data, infer_schema_length=1000)
            save_to_s3(hero_rankings_df, s3_client, bucket, f"{base_path}/hero_rankings.parquet")

        # Extração de cenários (situações no jogo)
        print("\nExtraindo cenários...")
        item_timings_data = api.get_item_timings()
        if item_timings_data:
            item_timings_df = create_dataframe_from_json(item_timings_data, infer_schema_length=1000)
            save_to_s3(item_timings_df, s3_client, bucket, f"{base_path}/item_timings.parquet")

        lane_roles_data = api.get_lane_roles()
        if lane_roles_data:
            lane_roles_df = create_dataframe_from_json(lane_roles_data, infer_schema_length=1000)
            save_to_s3(lane_roles_df, s3_client, bucket, f"{base_path}/lane_roles.parquet")

        misc_scenarios_data = api.get_misc_scenarios()
        if misc_scenarios_data:
            misc_scenarios_df = create_dataframe_from_json(misc_scenarios_data, infer_schema_length=1000)
            save_to_s3(misc_scenarios_df, s3_client, bucket, f"{base_path}/misc_scenarios.parquet")

        print(f"\nExtração concluída! Os dados foram salvos no diretório: s3://{bucket}/{base_path}")

        # Limpa arquivos temporários locais
        clean_local_files()

    except Exception as e:
        print(f"Erro durante a extração: {str(e)}")
        clean_local_files()  # Tenta limpar mesmo se ocorrer erro
        raise

# Ponto de entrada do script
if __name__ == "__main__":
    extract_all_data()
