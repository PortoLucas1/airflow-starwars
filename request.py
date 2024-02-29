import requests
import json
import os
from datetime import datetime
import pandas as pd

# List of Star Wars APIs to be consumed
url_peoples = 'https://swapi.dev/api/people/'
url_films = 'https://swapi.dev/api/films/'
url_vehicles = 'https://swapi.dev/api/vehicles/'

# Initializing the objects as empty lists
peoples = []
films = []
vehicles = []

# ------------PEOPLES-----------------

# Criação de lista com todo o conteúdo das paginações da API de peoples
page = 1
page_content = requests.get(url=url_peoples+'?page='+str(page)).json()
next_page = page_content['next']
peoples.append(page_content)

while (next_page != None):
    page += 1
    page_content = requests.get(url=url_peoples+'?page='+str(page)).json()
    next_page = page_content['next']
    peoples.append(page_content)

# Coleta de lista de anos dos itens consultados
years = []
df = pd.json_normalize(peoples, record_path=['results'])

for index, row in df.iterrows():
    year = datetime.strptime(row['created'], '%Y-%m-%dT%H:%M:%S.%fZ').year
    if year not in years:
        years.append(year)

dict = df.to_dict('records')

# Criação dos arquivos para cada ano
for year in years:
    if not os.path.exists(f"./files/peoples/{year}/"):
        os.makedirs(f"./files/peoples/{year}/")
        print("Directory created successfully")
    with open(f"./files/peoples/{year}/peoples.json", 'w', encoding='utf-8') as outfile:
        json.dump(dict, outfile, ensure_ascii=False, indent=4)

# ------------PEOPLES-----------------

"""



# ------------FILMS-----------------

# Writing films to ./files/films.json
page = 1
films_results = requests.get(url=url_films+'?page='+str(page)).json()['results']
years = []
for film in films_results:
    film_year = datetime.strptime(film['created'], '%Y-%m-%dT%H:%M:%S.%fZ').year
    if film_year not in years:
        years.append(film_year)

for year in years:
    if not os.path.exists(f"./files/films/{year}/"):
        os.makedirs(f"./files/films/{year}/")
        print("Directory created successfully")

page = 1
page_content = requests.get(url=url_films+'?page='+str(page)).json()
next_page = page_content['next']
films.append(page_content)

while (next_page != None):
    page += 1
    page_content = requests.get(url=url_films+'?page='+str(page)).json()
    next_page = films_page_content['next']
    films.append(page_content)

with open(f"./files/films/films.json", 'w', encoding='utf-8') as outfile:
    json.dump(films, outfile, ensure_ascii=False, indent=4)

# ------------FILMS-----------------


# ------------VEHICLES-----------------

# Writing vehicles to ./files/vehicles.json
page = 1
page_content = requests.get(url=url_vehicles+'?page='+str(page)).json()
next_page = page_content['next']
vehicles.append(page_content)

while (next_page != None):
    page += 1
    page_content = requests.get(url=url_vehicles+'?page='+str(page)).json()
    next_page = page_content['next']
    vehicles.append(page_content)

with open('./files/vehicles/vehicles.json', 'w', encoding='utf-8') as outfile:
    json.dump(vehicles, outfile, ensure_ascii=False, indent=4)

# ------------VEHICLES-----------------


"""