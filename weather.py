import datetime
from pymongo import MongoClient
import pandas as pd

mongo_url = 'mongodb://core3:654321@localhost:27017'


class WeatherReport(object):

    def __init__(self):
        self._conn = None

    @property
    def connection(self):
        if self._conn is None:
            self._conn = MongoClient(mongo_url)
        return self._conn['dwd']

    @property
    def station_collection(self):
        return self.connection['station']

    @property
    def metric_collection(self):
        return self.connection['metrik']

    @property
    def geo_collection(self):
        return self.connection['geo']

    def get_station(self, start, end):
        enriched_station = []
        _coll = self.station_collection
        cur_agg = _coll.aggregate([{'$match': {}},
                                 {'$unwind': '$_data'},
                                 {'$match': {'_data.von_datum': {'$lte': start},
                                             '_data.bis_datum': {'$gt': end + datetime.timedelta(days=1)}}}])
        #TODO: Check if open end station are correct : bis_date = None
        for station in cur_agg:
            doc = {}
            #for node in station['_data']:
                #if node['bis_datum'] is None:
            node = station['_data']
            geo_key = str(node['geogr_breite']) + ', ' + str(node['geogr_laenge'])
            doc['stationsnummer'] = station['_id']
            doc['stationsname'] = node['stationsname']
            doc['stationshoehe'] = node['stationshoehe']
            doc['geo_key'] = geo_key
            doc['geogr_breite'] = node['geogr_breite']
            doc['geogr_laenge'] = node['geogr_laenge']
            doc['von_datum'] = node['von_datum']
            doc['bis_datum'] = node['bis_datum']
            enriched_station.append(doc)
        return enriched_station

    def get_station_geo(self, start, end):
        """
        retrieves google maps information about the passed stations

        :return: list of station dict including google maps information
        """
        station = self.get_station(start, end)
        coll_geo = self.geo_collection

        cleaned_stations = [e for e in station if e['stationsnummer'] not in [954, 1228]]

        for doc in cleaned_stations:
            cur_geo = coll_geo.find({'_id': doc['geo_key']})
            for c in cur_geo:
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

        df_geo = pd.DataFrame(cleaned_stations)
        return df_geo

    def get_observed_metric(self, station, start, end):
        """
        retrieves information about the observed metrics

        :param station: list of station dict
        :param start: of time period
        :param end: of time period
        :return: list of station dict including observed metrics
        """
        summary = []
        for a in station:
            doc = {}
            doc['stationsnummer'] = a
            doc['metrics'] = self.metric_collection.distinct('_metric',
                                                             {
                                                                 'stations_id': a,
                                                                 'mess_datum': {
                                                                     '$gte': start,
                                                                     '$lt': end + datetime.timedelta(days=1)
                                                                 }
                                                             })
            summary.append(doc)

        summary = pd.DataFrame(summary)

        return summary


       # pass

#     def get_metric(self, station, start, end):
#         """
#         retrieves the metrics
#
#         :param station:
#         :param start:
#         :param end:
#         :return:
#         """
#         pass

    def retrieve(self, start, end):#, zip=[], rain=True, sun=True, air=True):
        # Get stations with geo-information
        station_df = self.get_station_geo(start, end)

        # TODO: Add the station filter-list here [zip codes], [Bundesländer]
        station_list = list(set([e for e in station_df.stationsnummer.unique()]))
        metrics = self.get_observed_metric(station_list, start, end)
        df = station_df.merge(metrics, how='left', on=['stationsnummer'])

        # TODO: Add the metrics filter-list here [....]
        return df


wr = WeatherReport()
stations = wr.retrieve(start=datetime.datetime(2016, 1, 1),
                       end=datetime.datetime(2016, 2, 1))

print(stations)
stations.to_csv("/tmp/WeatherApp_results___V01.csv",
                sep='\t',
                encoding='utf-8')
