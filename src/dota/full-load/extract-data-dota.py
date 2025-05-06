import polars as pl
from datetime import datetime
import pytz
from opendotaapi import OpenDotaAPI
import os
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
import json
import glob

def setup_aws_credentials():
    # Obtém as credenciais dos segredos do GitHub Actions
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    aws_region = os.environ.get('AWS_REGION', 'us-east-1')

    if not aws_access_key or not aws_secret_key:
        raise ValueError("Credenciais AWS não encontradas nos segredos do GitHub Actions")
    
    # Configura o cliente S3
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )
    
    return s3_client

def clean_local_files():
    """Limpa todos os arquivos e pastas criados localmente durante a execução"""
    try:
        # Lista de extensões de arquivos para deletar
        extensions = ['*.parquet', '*.json', '*.csv']
        
        # Deleta arquivos com as extensões especificadas
        for ext in extensions:
            files = glob.glob(ext)
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
    """Salva um DataFrame no S3 usando pyarrow"""
    try:
        # Converte o DataFrame Polars para PyArrow
        table = df.to_arrow()
        
        # Cria um buffer para armazenar o arquivo parquet
        buffer = pa.BufferOutputStream()
        
        # Escreve o arquivo parquet no buffer
        pq.write_table(table, buffer)
        
        # Obtém o conteúdo do buffer
        buffer_content = buffer.getvalue().to_pybytes()
        
        # Faz upload para o S3
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
    """Cria um DataFrame do Polars a partir de dados JSON com melhor tratamento de schema"""
    try:
        # Se os dados já são uma lista de dicionários, usa diretamente
        if isinstance(data, list) and all(isinstance(item, dict) for item in data):
            return pl.DataFrame(data, infer_schema_length=infer_schema_length)
        
        # Se os dados são uma string JSON, converte para lista de dicionários
        if isinstance(data, str):
            data = json.loads(data)
        
        # Se os dados são um dicionário único, converte para lista
        if isinstance(data, dict):
            data = [data]
        
        # Cria o DataFrame com schema mais flexível
        return pl.DataFrame(data, infer_schema_length=infer_schema_length)
    
    except Exception as e:
        print(f"Erro ao criar DataFrame: {str(e)}")
        print(f"Tipo dos dados: {type(data)}")
        if isinstance(data, list) and len(data) > 0:
            print(f"Exemplo do primeiro item: {data[0]}")
        raise

def extract_all_data():
    # Configura credenciais AWS
    s3_client = setup_aws_credentials()
    
    # Inicializa a API
    api = OpenDotaAPI()
    
    # Base path para os arquivos no S3
    bucket = "scarstimeslake"
    base_path = "dota/stage/api/full-load"
    
    print(f"Iniciando extração de dados do Dota 2...")
    
    try:
        # Extrai dados básicos
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
        
        # Extrai dados de times
        print("\nExtraindo dados de times...")
        teams_data = api.get_teams()
        if teams_data:
            teams_df = create_dataframe_from_json(teams_data, infer_schema_length=1000)
            save_to_s3(teams_df, s3_client, bucket, f"{base_path}/teams.parquet")
        
        # Extrai dados de ligas
        print("\nExtraindo dados de ligas...")
        leagues_data = api.get_leagues()
        if leagues_data:
            leagues_df = create_dataframe_from_json(leagues_data, infer_schema_length=1000)
            save_to_s3(leagues_df, s3_client, bucket, f"{base_path}/leagues.parquet")
        
        # Extrai dados de jogadores profissionais
        print("\nExtraindo dados de jogadores profissionais...")
        pro_players_data = api.get_pro_players()
        if pro_players_data:
            pro_players_df = create_dataframe_from_json(pro_players_data, infer_schema_length=1000)
            save_to_s3(pro_players_df, s3_client, bucket, f"{base_path}/pro_players.parquet")
        
        # Extrai dados de partidas profissionais
        print("\nExtraindo dados de partidas profissionais...")
        pro_matches_data = api.get_pro_matches()
        if pro_matches_data:
            pro_matches_df = create_dataframe_from_json(pro_matches_data, infer_schema_length=1000)
            save_to_s3(pro_matches_df, s3_client, bucket, f"{base_path}/pro_matches.parquet")
        
        # Extrai rankings de heróis
        print("\nExtraindo rankings de heróis...")
        hero_rankings_data = api.get_hero_rankings()
        if hero_rankings_data:
            hero_rankings_df = create_dataframe_from_json(hero_rankings_data, infer_schema_length=1000)
            save_to_s3(hero_rankings_df, s3_client, bucket, f"{base_path}/hero_rankings.parquet")
        
        # Extrai cenários
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
        
        # Limpa arquivos locais após a extração
        clean_local_files()
        
    except Exception as e:
        print(f"Erro durante a extração: {str(e)}")
        # Tenta limpar arquivos mesmo em caso de erro
        clean_local_files()
        raise

if __name__ == "__main__":
    extract_all_data() 