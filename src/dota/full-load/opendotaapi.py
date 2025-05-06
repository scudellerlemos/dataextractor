import polars as pl
import requests
from datetime import datetime
import pytz
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class OpenDotaAPI:
    def __init__(self):
        self.base_url = "https://api.opendota.com/api"
        self.url_base = f"{self.base_url}/publicMatches"
        self.url_clusters = f"{self.base_url}/constants/cluster"
        self.url_heroes = f"{self.base_url}/heroes"
        self.url_lobby_types = f"{self.base_url}/constants/lobby_type"
        self.url_game_modes = f"{self.base_url}/constants/game_mode"
        
        # Configuração do cliente HTTP com retry
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,  # número total de tentativas
            backoff_factor=1,  # tempo de espera entre tentativas
            status_forcelist=[429, 500, 502, 503, 504],  # códigos de status para retry
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
    def _make_request(self, url, params=None):
        """Método auxiliar para fazer requisições com retry e tratamento de erros"""
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()  # Levanta exceção para códigos de erro HTTP
            
            # Verifica se a resposta está vazia
            if not response.content:
                print(f"Aviso: Resposta vazia da API para {url}")
                return None
                
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"Endpoint não encontrado: {url}")
            elif e.response.status_code == 429:
                print("Rate limit atingido. Aguardando 60 segundos...")
                time.sleep(60)
                return self._make_request(url, params)  # Tenta novamente após esperar
            else:
                print(f"Erro HTTP ao acessar {url}: {str(e)}")
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"Erro ao fazer requisição para {url}: {str(e)}")
            return None
            
        except ValueError as e:
            print(f"Erro ao decodificar JSON da resposta de {url}: {str(e)}")
            return None
        
    def get_matches(self):
        try:
            matches = self._make_request(self.url_base)
            if matches:
                for partida in matches:
                    if 'radiant_team' in partida and 'dire_team' in partida:
                        for i, hero_id in enumerate(partida['radiant_team'], 1):
                            partida[f'radiant_hero_{i}'] = hero_id
                        for i, hero_id in enumerate(partida['dire_team'], 1):
                            partida[f'dire_hero_{i}'] = hero_id
                        del partida['radiant_team']
                        del partida['dire_team']
                df = pl.DataFrame(matches)
                br_tz = pytz.timezone('America/Sao_Paulo')
                data_br = datetime.fromtimestamp(df['start_time'][0], tz=pytz.UTC).astimezone(br_tz)
                nome_arquivo = data_br.strftime('%Y-%m-%d-dadosprincipal.parquet')
                df.write_parquet(nome_arquivo)
                print(f"\nDataset com {len(df)} partidas salvo em {nome_arquivo}")
                return df
            return None
        except Exception as e:
            print(f"Erro ao processar partidas: {str(e)}")
            return None

    # Métodos existentes
    def get_lobby_types(self):
        try:
            response = requests.get(self.url_lobby_types)
            if response.status_code == 200:
                lobby_types = response.json()
                lobby_types_list = [{"lobby_id": k, "name": v} for k,v in lobby_types.items()]
                df = pl.DataFrame(lobby_types_list)
                df.write_parquet("lobby_types.parquet")
                return df
            return None
        except Exception as e:
            print(f"Erro ao fazer requisição de tipos de lobby: {str(e)}")
            return None

    def get_game_modes(self):
        try:
            response = requests.get(self.url_game_modes)
            if response.status_code == 200:
                game_modes = response.json()
                game_modes_list = [{"mode_id": k, "name": v} for k,v in game_modes.items()]
                df = pl.DataFrame(game_modes_list)
                df.write_parquet("game_modes.parquet")
                return df
            return None
        except Exception as e:
            print(f"Erro ao fazer requisição de modos de jogo: {str(e)}")
            return None

    def get_clusters(self):
        try:
            response = requests.get(self.url_clusters)
            if response.status_code == 200:
                clusters = response.json()
                clusters_list = [{"cluster_id": k, "name": v} for k,v in clusters.items()]
                df = pl.DataFrame(clusters_list)
                df.write_parquet("clusters.parquet")
                return df
            return None
        except Exception as e:
            print(f"Erro ao fazer requisição de clusters: {str(e)}")
            return None

    def get_heroes(self):
        try:
            response = requests.get(self.url_heroes)
            if response.status_code == 200:
                heroes = response.json()
                df = pl.DataFrame(heroes)
                df.write_parquet("heroes.parquet")
                return df
            return None
        except Exception as e:
            print(f"Erro ao fazer requisição de heróis: {str(e)}")
            return None

    # Novos métodos para os endpoints solicitados
    def get_match_details(self, match_id):
        try:
            response = requests.get(f"{self.base_url}/matches/{match_id}")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter detalhes da partida: {str(e)}")
            return None

    def get_match_players(self, match_id):
        try:
            response = requests.get(f"{self.base_url}/matches/{match_id}/players")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter jogadores da partida: {str(e)}")
            return None

    def get_match_timeline(self, match_id):
        try:
            response = requests.get(f"{self.base_url}/matches/{match_id}/timeline")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter timeline da partida: {str(e)}")
            return None

    def get_match_chat(self, match_id):
        try:
            response = requests.get(f"{self.base_url}/matches/{match_id}/chat")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter chat da partida: {str(e)}")
            return None

    def get_hero_stats(self, hero_id):
        try:
            response = requests.get(f"{self.base_url}/heroes/{hero_id}/stats")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter estatísticas do herói: {str(e)}")
            return None

    def get_hero_durations(self, hero_id):
        try:
            response = requests.get(f"{self.base_url}/heroes/{hero_id}/durations")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter durações do herói: {str(e)}")
            return None

    def get_hero_players(self, hero_id):
        try:
            response = requests.get(f"{self.base_url}/heroes/{hero_id}/players")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter jogadores do herói: {str(e)}")
            return None

    def get_leagues(self):
        try:
            response = requests.get(f"{self.base_url}/leagues")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter ligas: {str(e)}")
            return None

    def get_league_details(self, league_id):
        try:
            response = requests.get(f"{self.base_url}/leagues/{league_id}")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter detalhes da liga: {str(e)}")
            return None

    def get_league_matches(self, league_id):
        try:
            response = requests.get(f"{self.base_url}/leagues/{league_id}/matches")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter partidas da liga: {str(e)}")
            return None

    def get_teams(self):
        try:
            response = requests.get(f"{self.base_url}/teams")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter times: {str(e)}")
            return None

    def get_team_details(self, team_id):
        try:
            response = requests.get(f"{self.base_url}/teams/{team_id}")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter detalhes do time: {str(e)}")
            return None

    def get_team_matches(self, team_id):
        try:
            response = requests.get(f"{self.base_url}/teams/{team_id}/matches")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter partidas do time: {str(e)}")
            return None

    def get_team_players(self, team_id):
        try:
            response = requests.get(f"{self.base_url}/teams/{team_id}/players")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter jogadores do time: {str(e)}")
            return None

    def get_team_heroes(self, team_id):
        try:
            response = requests.get(f"{self.base_url}/teams/{team_id}/heroes")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter heróis do time: {str(e)}")
            return None

    def get_explorer(self):
        try:
            response = requests.get(f"{self.base_url}/explorer")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter explorer: {str(e)}")
            return None

    def get_explorer_schema(self):
        try:
            response = requests.get(f"{self.base_url}/explorer/schema")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter schema do explorer: {str(e)}")
            return None

    def get_distributions(self):
        try:
            response = requests.get(f"{self.base_url}/distributions")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter distribuições: {str(e)}")
            return None

    def get_status(self):
        try:
            response = requests.get(f"{self.base_url}/status")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter status: {str(e)}")
            return None

    def get_health(self):
        try:
            response = requests.get(f"{self.base_url}/health")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter health: {str(e)}")
            return None

    def get_metadata(self):
        try:
            response = requests.get(f"{self.base_url}/metadata")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter metadata: {str(e)}")
            return None

    def get_pro_players(self):
        try:
            response = requests.get(f"{self.base_url}/proPlayers")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter pro players: {str(e)}")
            return None

    def get_pro_matches(self):
        try:
            response = requests.get(f"{self.base_url}/proMatches")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter pro matches: {str(e)}")
            return None

    def get_public_players(self):
        try:
            response = requests.get(f"{self.base_url}/public/players")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter jogadores públicos: {str(e)}")
            return None

    def get_rankings(self):
        try:
            response = requests.get(f"{self.base_url}/rankings")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter rankings: {str(e)}")
            return None

    def get_hero_rankings(self):
        try:
            response = requests.get(f"{self.base_url}/heroes")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter rankings de heróis: {str(e)}")
            return None

    def get_item_timings(self):
        try:
            response = requests.get(f"{self.base_url}/scenarios/itemTimings")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter timings de itens: {str(e)}")
            return None

    def get_lane_roles(self):
        try:
            response = requests.get(f"{self.base_url}/scenarios/laneRoles")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter roles de lanes: {str(e)}")
            return None

    def get_misc_scenarios(self):
        try:
            response = requests.get(f"{self.base_url}/scenarios/misc")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter cenários misc: {str(e)}")
            return None

    def get_schema(self):
        try:
            response = requests.get(f"{self.base_url}/schema")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter schema: {str(e)}")
            return None

    def get_constants(self):
        try:
            response = requests.get(f"{self.base_url}/constants")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"Erro ao obter constants: {str(e)}")
            return None 