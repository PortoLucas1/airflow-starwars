import requests
import json
import os
from datetime import datetime

# List of Star Wars APIs to be consumed
url_peoples = 'https://swapi.dev/api/people/'
url_films = 'https://swapi.dev/api/films/'
url_vehicles = 'https://swapi.dev/api/vehicles/'

# Initializing the objects as empty lists
peoples = []
films = []
vehicles = []


# ------------PEOPLES-----------------

page = 1
page_content = requests.get(url=url_peoples+'?page='+str(page)).json()
next_page = page_content['next']
peoples.append(page_content)

while (next_page != None):
    page += 1
    page_content = requests.get(url=url_peoples+'?page='+str(page)).json()
    next_page = page_content['next']
    peoples.append(page_content)

# Writing peoples to ./files/peoples.json
with open('./files/peoples/peoples.json', 'w', encoding='utf-8') as outfile:
    json.dump(peoples, outfile, ensure_ascii=False, indent=4)

# ------------PEOPLES-----------------


# ------------FILMS-----------------

# Writing films to ./files/films.json
"""
page = 1
films_results = requests.get(url=url_films+'?page='+str(page)).json()['results']
years = []
for film in films_results:
    film_year = datetime.strptime(film['created'], '%Y-%m-%dT%H:%M:%S.%fZ').year
    if film_year not in years:
        years.append(film_year)
"""

"""
for year in years:
    if not os.path.exists(f"./files/films/{year}/"):
        os.makedirs(f"./files/films/{year}/")
        print("Directory created successfully")
"""

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