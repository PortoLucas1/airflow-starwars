from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from airflow import DAG

from datetime import datetime 

import requests
import json
import os
import pandas as pd

# URL de base para as APIs
base_url = 'https://swapi.py4e.com/api/'

# Paths relativos às APIs consumidas
paths = ['people', 'films', 'vehicles']

@dag(
    dag_id='dag-starwars',
    start_date=datetime(2024,3,13),
    schedule=None
)
def dag_starwars():

    @task
    def coleta_de_dados(url):
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

            # Dataframes auxiliares para utilização posterior
            if path == paths[0]:
                df_people = df
            elif path == paths[1]:
                df_films = df
            else:
                df_vehicles = df

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
                with open(f"./files/{path}/{year}/{path}.json", 'w', encoding='utf-8') as outfile:
                    json.dump(dict, outfile, ensure_ascii=False, indent=4)
    
    coleta_de_dados = coleta_de_dados(base_url)
    
dag_starwars = dag_starwars()