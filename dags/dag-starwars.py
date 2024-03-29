from airflow.decorators import dag, task

from datetime import datetime 

import requests
import json
import os
import pandas as pd

# URL de base para as APIs
# base_url = 'https://swapi.py4e.com/api/'
base_url = 'https://swapi.dev/api/'

# Paths relativos às APIs consumidas
paths = ['people', 'films', 'vehicles']

@dag(
    dag_id='dag-starwars',
    start_date=datetime(2024,3,14),
    schedule=None
)
def dag_starwars():

    @task
    def coleta_dados(url):
        # Iteração em cada path, para coleta de dados
        for path in paths:
            results = []
            page = 1
            page_content = requests.get(url=url+'/'+path+'/'+'?page='+str(page)).json()
            next_page = page_content['next']
            results.append(page_content)

            # Checagem de paginação adicional, para continuação da coleta
            while (next_page != None):
                page += 1
                page_content = requests.get(url=url+'/'+path+'/'+'?page='+str(page)).json()
                next_page = page_content['next']
                results.append(page_content)

            df = pd.json_normalize(results, record_path=['results'])

            # Coleta de lista de anos dos itens consultados
            df['year'] = pd.to_datetime(df.created, format='mixed').dt.year
            years = df['year'].unique().tolist()
            
            # Agrupamemnto por ano, para separação em arquivos
            grouped = df.groupby(df.year)

            # Criação dos arquivos para cada ano
            for year in years:
                if not os.path.exists(f"./files/{path}/{year}/"):
                    os.makedirs(f"./files/{path}/{year}/")
                    print("Directory created successfully")
                # Segmentação por ano
                df_prov = grouped.get_group(year)
                df_prov = df_prov.drop(['year'], axis=1)
                dict = df_prov.to_dict('records')
                print(dict)
                with open(f"./files/{path}/{year}/{path}.json", 'w', encoding='utf-8') as outfile:
                    json.dump(dict, outfile, ensure_ascii=False, indent=4)
    
    @task
    def contagem_personagens():
        def list_folders(directory):
            for folder_name in os.listdir(directory):
                if os.path.isdir(os.path.join(directory, folder_name)):
                    yield folder_name

        # Coleta do dataframe auxiliar
        df_people = pd.DataFrame()
        people_directory_path = "./files/people"
        for folder in list_folders(people_directory_path):
            df_people = pd.concat([df_people, pd.read_json(f"./files/people/{folder}/people.json")], ignore_index=True)

        # Quantidade de registros retornados no endpoint people
        print(f"A quantidade de registros retornada foi: {df_people.name.count()}")

    @task
    def participacao_filmes():
        def list_folders(directory):
            for folder_name in os.listdir(directory):
                if os.path.isdir(os.path.join(directory, folder_name)):
                    yield folder_name

        # Coleta dos dataframes auxiliares
        df_people = pd.DataFrame()
        people_directory_path = "./files/people"
        for folder in list_folders(people_directory_path):
            df_people = pd.concat([df_people, pd.read_json(f"./files/people/{folder}/people.json")], ignore_index=True)

        df_films = pd.DataFrame()
        films_directory_path = "./files/films"
        for folder in list_folders(films_directory_path):
            df_films = pd.concat([df_films, pd.read_json(f"./files/films/{folder}/films.json")], ignore_index=True)

        # Criação do arquivo cast.json com títulos dos filmes para os personagens
        list_of_films = []
        filtered_list = []

        for index, row in df_people.iterrows():
            film_ids = []
            film_ids = row["films"]
            filtered_list = [df_films.title[df_films.url == item].values[0] for item in df_films.url.tolist() if item in film_ids]
            list_of_films.append(filtered_list)

        # Transformação e ajuste dos dados
        df_people.films = list_of_films
        df_people.rename(columns={"films": "titles"}, inplace=True)
        df_people_cast = df_people[['name', 'height', 'gender', 'titles']]
        dict = df_people_cast.to_dict('records')

        print(dict)

        if not os.path.exists(f"./files/cast/"):
            os.makedirs(f"./files/cast/")
            print("Directory created successfully")
        with open(f"./files/cast/cast.json", 'w', encoding='utf-8') as outfile:
            json.dump(dict, outfile, ensure_ascii=False, indent=4)

    # Instanciação de tasks
    coleta_dados = coleta_dados(base_url)
    contagem_personagens = contagem_personagens()
    participacao_filmes = participacao_filmes()

    # Dependência de tasks
    coleta_dados >> contagem_personagens >> participacao_filmes
    
dag_starwars = dag_starwars()