import pymongo
import pandas as pd
import pprint

url = 'mongodb://user:pwd@server:27017'
mongo = pymongo.MongoClient(url)

coll_station = mongo.dwd.station
coll_geo = mongo.dwd.geo

station_query = {}

cur_station = coll_station.find(station_query)
enriched_station = []
for station in cur_station:
    doc = {}
    for node in station['_data']:
        if node['bis_datum'] is None:
            geo_key = str(node['geogr_breite']) + ', ' + str(node['geogr_laenge'])
            pprint.pprint(geo_key)
            doc['stationsnummer'] = station['_id']
            doc['stationsname'] = node['stationsname']
            doc['stationshoehe'] = node['stationshoehe']
            doc['geo_key'] = geo_key
            doc['geogr_breite'] = node['geogr_breite']
            doc['geogr_laenge'] = node['geogr_laenge']
            doc['von_datum'] = node['von_datum']
            doc['bis_datum'] = node['bis_datum']
            enriched_station.append(doc)

# Do not consider the boats on the open sea....
cleaned_stations = [e for e in enriched_station if e['stationsnummer'] not in [954, 1228]]
# TODO: Remove all station that are situated over X meter

for doc in cleaned_stations:
    cur_geo = coll_geo.find({'_id': doc['geo_key']})
    for c in cur_geo:
        try:
            for node in c['data']:
                if node['types'] in [[u'route'], [u'street_address']]:
                    add = {}
                    for ad in node['address_components']:
                        if ad['types'] == [u'route']:
                            add['strasse'] = ad[u'long_name']
                        elif ad['types'] == [u'street_number']:
                            add['strassennummer'] = ad[u'long_name']
                        elif ad['types'] == [u'locality', u'political']:
                            add['gemeinde'] = ad[u'long_name']
                        elif ad['types'] == [u'administrative_area_level_2', u'political']:
                            add['administrative_area_level_2'] = ad[u'long_name']
                            add['administrative_area_level_2_short'] = ad[u'short_name']
                        elif ad['types'] == [u'administrative_area_level_1', 'political']:
                            add['bundesland'] = ad[u'long_name']
                            add['bundesland_kurz'] = ad[u'short_name']
                        elif ad['types'] == [u'postal_code']:
                            add['postnummer'] = ad[u'long_name']
                        else:
                            pass
                    doc['address'] = add
        except Exception as err:
            pprint.pprint(err)

df = pd.DataFrame(cleaned_stations)
df.to_csv('/tmp/station_with_geo_info.csv',
          encoding='iso-8859-1',
          sep=';',
          index=False)
