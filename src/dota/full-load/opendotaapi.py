import polars as pl
import requests
from datetime import datetime
import pytz
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class OpenDotaAPI:
    """
    Classe para interação com a API pública do OpenDota.

    Funcionalidades principais:
    - Coleta de partidas públicas
    - Consulta de heróis, modos de jogo, clusters, tipos de lobby
    - Consulta detalhada de partidas e heróis
    - Download de arquivos em formato .parquet
    """

    def __init__(self):
        self.base_url = "https://api.opendota.com/api"
        self.url_base = f"{self.base_url}/publicMatches"
        self.url_clusters = f"{self.base_url}/constants/cluster"
        self.url_heroes = f"{self.base_url}/heroes"
        self.url_lobby_types = f"{self.base_url}/constants/lobby_type"
        self.url_game_modes = f"{self.base_url}/constants/game_mode"
        
        # Configuração de sessão com retry automático
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _make_request(self, url, params=None):
        """
        Realiza uma requisição GET com tratamento de erros e retry automático.

        Args:
            url (str): URL do endpoint da API
            params (dict): Parâmetros da requisição GET

        Returns:
            dict/list: Resposta da API em formato JSON, ou None em caso de erro
        """
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()

            if not response.content:
                print(f"[AVISO] Resposta vazia para {url}")
                return None

            return response.json()
        
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"[ERRO 404] Endpoint não encontrado: {url}")
            elif e.response.status_code == 429:
                print("[ERRO 429] Rate limit excedido. Aguardando 60 segundos...")
                time.sleep(60)
                return self._make_request(url, params)
            else:
                print(f"[ERRO HTTP] Falha na requisição {url}: {str(e)}")
            return None
        
        except requests.exceptions.RequestException as e:
            print(f"[ERRO DE CONEXÃO] Falha ao conectar em {url}: {str(e)}")
            return None
        
        except ValueError as e:
            print(f"[ERRO DE JSON] Falha ao interpretar JSON de {url}: {str(e)}")
            return None

    def get_matches(self):
        """
        Coleta partidas públicas recentes e salva como arquivo .parquet.

        Returns:
            pl.DataFrame: DataFrame com as partidas coletadas
        """
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

                # Formata o nome do arquivo com a data da partida mais recente
                br_tz = pytz.timezone('America/Sao_Paulo')
                data_br = datetime.fromtimestamp(df['start_time'][0], tz=pytz.UTC).astimezone(br_tz)
                nome_arquivo = data_br.strftime('%Y-%m-%d-dadosprincipal.parquet')
                df.write_parquet(nome_arquivo)

                print(f"[SUCESSO] Dataset com {len(df)} partidas salvo em {nome_arquivo}")
                return df
            return None
        except Exception as e:
            print(f"[ERRO] Falha ao processar partidas: {str(e)}")
            return None

    # Métodos para constantes

    def get_lobby_types(self):
        """Retorna os tipos de lobby do jogo"""
        try:
            response = self._make_request(self.url_lobby_types)
            if response:
                lista = [{"lobby_id": k, "name": v} for k, v in response.items()]
                df = pl.DataFrame(lista)
                df.write_parquet("lobby_types.parquet")
                return df
            return None
        except Exception as e:
            print(f"[ERRO] Tipos de lobby: {str(e)}")
            return None

    def get_game_modes(self):
        """Retorna os modos de jogo disponíveis"""
        try:
            response = self._make_request(self.url_game_modes)
            if response:
                lista = [{"mode_id": k, "name": v} for k, v in response.items()]
                df = pl.DataFrame(lista)
                df.write_parquet("game_modes.parquet")
                return df
            return None
        except Exception as e:
            print(f"[ERRO] Modos de jogo: {str(e)}")
            return None

    def get_clusters(self):
        """Retorna os clusters (data centers) do jogo"""
        try:
            response = self._make_request(self.url_clusters)
            if response:
                lista = [{"cluster_id": k, "name": v} for k, v in response.items()]
                df = pl.DataFrame(lista)
                df.write_parquet("clusters.parquet")
                return df
            return None
        except Exception as e:
            print(f"[ERRO] Clusters: {str(e)}")
            return None

    def get_heroes(self):
        """Retorna a lista de heróis disponíveis"""
        try:
            response = self._make_request(self.url_heroes)
            if response:
                df = pl.DataFrame(response)
                df.write_parquet("heroes.parquet")
                return df
            return None
        except Exception as e:
            print(f"[ERRO] Heróis: {str(e)}")
            return None

    # Métodos de partidas detalhadas

    def get_match_details(self, match_id):
        """Detalhes completos de uma partida específica"""
        return self._make_request(f"{self.base_url}/matches/{match_id}")

    def get_match_players(self, match_id):
        """Lista de jogadores de uma partida"""
        return self._make_request(f"{self.base_url}/matches/{match_id}/players")

    def get_match_timeline(self, match_id):
        """Eventos cronológicos da partida"""
        return self._make_request(f"{self.base_url}/matches/{match_id}/timeline")

    def get_match_chat(self, match_id):
        """Mensagens de chat da partida"""
        return self._make_request(f"{self.base_url}/matches/{match_id}/chat")

    # Métodos de heróis

    def get_hero_stats(self, hero_id):
        """Estatísticas de desempenho do herói"""
        return self._make_request(f"{self.base_url}/heroes/{hero_id}/stats")

    def get_hero_durations(self, hero_id):
        """Distribuição de partidas do herói por duração"""
        return self._make_request(f"{self.base_url}/heroes/{hero_id}/durations")

    def get_hero_players(self, hero_id):
        """Jogadores que usam o herói"""
        return self._make_request(f"{self.base_url}/heroes/{hero_id}/players")

    # Métodos de ligas

    def get_leagues(self):
        """Retorna todas as ligas cadastradas"""
        return self._make_request(f"{self.base_url}/leagues")

    def get_league_details(self, league_id):
        """Retorna detalhes de uma liga específica"""
        return self._make_request(f"{self.base_url}/leagues/{league_id}")

    def get_league_matches(self, league_id):
        """Lista de partidas que pertencem à liga"""
        return self._make_request(f"{self.base_url}/leagues/{league_id}/matches")
