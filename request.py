import requests
import json
import os
from datetime import datetime
import pandas as pd

# Base URL for the APIs to be consumed
base_url = 'https://swapi.py4e.com/api/'

# Paths for the APIs to be consumed
paths = ['people', 'films', 'vehicles']

# Iteration over each path, for dat collection
for path in paths:
    # Initializing the objects as empty lists
    results = []
    page = 1
    page_content = requests.get(url=base_url+'/'+path+'/'+'?page='+str(page)).json()
    next_page = page_content['next']
    results.append(page_content)

    while (next_page != None):
        page += 1
        page_content = requests.get(url=base_url+'/'+path+'/'+'?page='+str(page)).json()
        next_page = page_content['next']
        results.append(page_content)

    # Coleta de lista de anos dos itens consultados
    df = pd.json_normalize(results, record_path=['results'])

    # Auxiliary dataframes
    if path == paths[0]:
        df_people = df
    elif path == paths[1]:
        df_films = df
    else:
        df_vehicles = df

    df['year'] = pd.to_datetime(df.created, format='mixed').dt.year
    grouped = df.groupby(df.year)

    years = df['year'].unique().tolist()

    # Criação dos arquivos para cada ano
    for year in years:
        if not os.path.exists(f"./files/{path}/{year}/"):
            os.makedirs(f"./files/{path}/{year}/")
            print("Directory created successfully")
        # Segmentando por ano
        df_prov = grouped.get_group(year)
        df_prov = df_prov.drop(['year'], axis=1)
        dict = df_prov.to_dict('records')
        with open(f"./files/{path}/{year}/{path}.json", 'w', encoding='utf-8') as outfile:
            json.dump(dict, outfile, ensure_ascii=False, indent=4)


print(f"A quantidade de registros retornada foi: {df_people.name.count()}")
list_of_films = []
# Salvar em um arquivo json o nome de 1 pessoa e os filmes associados a ela.
for index, row in df_people.iterrows():
    film_ids = []
    for film in row["films"]:
        film_ids.append(int(film[-2]))
    filtered_list = [df_films.title[df_films.episode_id == item].values[0] for item in df_films.episode_id.tolist() if item in film_ids]
    list_of_films.append(filtered_list)

df_people.films = list_of_films

df_people_cast = df_people[['name', 'films']]
dict = df_people_cast.to_dict('records')
with open(f"./files/people/cast/people.json", 'w', encoding='utf-8') as outfile:
            json.dump(dict, outfile, ensure_ascii=False, indent=4)