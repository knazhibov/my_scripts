import requests
from bs4 import BeautifulSoup
import fake_useragent
import geopandas as gpd
import json
import geojson
import shapely.wkt
from geojson import FeatureCollection
import osm2geojson
import geopandas as gpd
import pandas as pd
import csv
from tqdm import tqdm
import os
from shapely.ops import polygonize

# получение csrf токена
def get_csrf_token():
    url_for_token = 'https://www.openstreetmap.org/search?query=Москва=1'
    user = fake_useragent.UserAgent().random
    header = {
        'user-agent': user
    }
    
    #get запрос для получения csrf токена
    session = requests.Session()
    session.headers.update(header)
    responce = session.get(url_for_token)
    soup = BeautifulSoup(responce.content, 'html.parser')
    csrf = soup.find('meta', {'name': 'csrf-token'})['content']
    
    return header, session, csrf

# фильтрация по паттерну: город, село, дорога и тд.
def validation(name, patterns=None):
    if patterns is None:
        return True
    else:
        return any([name.lower().find(pattern) != -1 for pattern in patterns])

def get_polygons(output_file, names, patterns, log_areas=False):
    #словарь для post запроса
    header, session, token = get_csrf_token()
    data = {'zoom': "11",
            'minlon': "33.89076232910157",
            'minlat': "56.2082315947312",
            'maxlon': "34.820480346679695",
            'maxlat': "56.320344697649524",
            'authenticity_token': token}
    
    for name in tqdm(names):
        id = 0
        url = f'https://www.openstreetmap.org/geocoder/search_osm_nominatim?query={name}'

        #получаем id с openstreetmap для получения xml
        responce = session.post(url, data=data, headers=header)
        if responce.status_code == 200:
            soup = BeautifulSoup(responce.content, 'html.parser')
        else:
            print(f'Не удалось найти ни одного объекта с названием {name}')
            continue
        
        # получаем набор ответов по запросу, берем первый, подходящий под условия
        info = soup.findAll('a', {'class': 'set_position stretched-link'})
        areas = [(soup['href'], soup['data-name']) for soup in info]
        for area in areas:
            if validation(area[1], patterns) and area[0].split('/')[-2] != 'node':
                id = area[0]
                break
          
        if log_areas:
            print(name)
            print(areas)

        # проверяем, что нашлась нужная локация
        if id != 0:
            postfix = '/full'
            api_url = 'https://www.openstreetmap.org/api/0.6'+ id + postfix

            #get запрос на xml с полигоном
            responce = session.get(api_url)
            if responce.status_code == 200:
                xml_raw = BeautifulSoup(responce.content, "xml")
            else:
                print(f'Для локации {name} не удалось получить xml')
                continue
            
            # конвертация xml в geojson
            xml = str(xml_raw).split('<?xml version="1.0" encoding="utf-8"?>')[1]
            gj = osm2geojson.xml2geojson(xml)
            gj_df = gpd.GeoDataFrame.from_features(gj)
            gj_df = pd.DataFrame(gj_df, copy=False)

            # фильтруем на полигоны и мультилинейные строки, из последних делаем полигоны
            indicator = pd.Series(
                [any([
                    isinstance(x, shapely.geometry.polygon.Polygon),
                    isinstance(x, shapely.geometry.multilinestring.MultiLineString),
                    isinstance(x, shapely.geometry.multipolygon.MultiPolygon)
                ])
                for x in list(gj_df['geometry'])])
            gj_df = gj_df[indicator]
            gj_df['geometry'] = (
                gj_df['geometry']
                .apply(
                    lambda x: list(polygonize(list(x.geoms))) 
                    if not isinstance(x, shapely.geometry.Polygon)
                    else [x]
                )
            )
            gj_df = gj_df.explode('geometry')

            if len(gj_df) == 0:
                print(f'Не удалось выделить полигон для названия - {name}, попробуйте обновить паттерны и запустить снова')
            elif len(gj_df) == 1:
                key = list(dict(gj_df['tags']).keys())[0]
                place_names = []
                responce_names = dict(gj_df['tags'])[key]
                if 'name' in responce_names.keys():
                    place_names.append(responce_names['name'])
                elif 'name:ru' in responce_names.keys():
                    place_names.append(responce_names['name'])
                elif 'official_name' in responce_names.keys():
                    place_names.append(responce_names['name'])
                else:
                    place_name.append(name) 
            else:
                key = list(dict(gj_df['tags']).keys())[0]
                rows = dict(gj_df['tags'])[key].tolist()
                place_names = []
                for row in rows:
                    if 'name' in row.keys():
                        place_names.append(row['name'])
                    elif 'name:ru' in row.keys():
                        place_names.append(row['name'])
                    elif 'official_name' in row.keys():
                        place_names.append(row['name'])
                    else:
                        place_name.append(name)

            df = (
                pd.DataFrame(
                    {'place_names': place_names, 'geometry': gj_df.geometry.tolist()}, 
                    index=range(len(place_names))
                )
                .drop_duplicates(subset='geometry')
            )
            df['polygon_id'] = pd.Series(range(1, len(df) + 1))
            geometry = df.geometry.tolist()
            place_names = df.place_names.tolist()
            polygon_id = df.polygon_id.tolist()
            
            if not os.path.isfile(output_file):
                with open(output_file, mode="w", newline="", encoding="utf-8-sig") as file:
                    writer = csv.DictWriter(file, fieldnames=["place_name", "geometry", "polygon_id"])
                    writer.writeheader()
            # запись в файл csv
            for place_name, geom, id in zip(place_names, geometry, polygon_id):
                with open(output_file, mode="a", newline="", encoding="utf-8-sig") as file:
                    writer = csv.DictWriter(file, fieldnames=["place_name", "geometry", "polygon_id"])
                    writer.writerow({"place_name": place_name, "geometry": geom, "polygon_id": id})
        else:
            print(f'Не удалось найти подходящую локацию в OSM по названию {name}')

if __name__ == '__main__':          
    names =  ['МГТУ'] # подставить свои целевые названия геолокаций либо адреса 
    output_file = 'path.csv' # путь сохранения файла с результатом парсинга
    patterns = None  # подставить паттерны для фильтрации геолокаций
    get_polygons(output_file, names, patterns)