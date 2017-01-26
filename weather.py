# coding=utf-8
import datetime
from pymongo import MongoClient
import pandas as pd
import numpy as np

import core.queue.job

mongo_url = 'mongodb://core3:654321@localhost:27017'


class WeatherReport(core.queue.job.Job):
    def __init__(self):
        self._conn = None

    # establish a connection to the database through mogo_url
    @property
    def connection(self):
        if self._conn is None:
            self._conn = MongoClient(mongo_url)
        return self._conn['dwd']

    # retrieve station collection  from dwd
    @property
    def station_collection(self):
        return self.connection['station']

    # retrieve metric collection from dwd
    @property
    def metric_collection(self):
        return self.connection['metrik']

    # retrieve geo collection from dwd
    @property
    def geo_collection(self):
        return self.connection['geo']

    def create_metric_list(self,metric,metricList):
        """

        :param metric:  a string that indicates the name of the metric
        :param metricList: list of metrics (retrieved by get_observed_metric function)
        :return: list of boolean that indicates whether the metric exists in the metricList or not
        """
        metricCol = []
        for i in range(0,(len(metricList))):
            if metric in metricList[i]:
                metricCol.append(True)
            else:
                metricCol.append(False)

        return metricCol

    def get_station(self, start, end):
        """

        :param start: start of time period
        :param end: end of time period
        :return: list of active stations in this time, inclusding a key match for the geo collection
        """
        enriched_station = []
        _coll = self.station_collection
        cur_agg = _coll.aggregate([{'$match': {}},
                                   {'$unwind': '$_data'},
                                   {'$match': {'_data.von_datum': {'$lte': start},
                                               '_data.bis_datum': {'$gt': end + datetime.timedelta(days=1)}}}])
        # TODO: Check if open end station are correct : bis_date = None
        # for each of the stations we need to construct a geo_key that allows matching with the geo collection
        for station in cur_agg:
            doc = {}
            # for node in station['_data']:
            # if node['bis_datum'] is None:
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
        #split the metrics column into three columns for each metric
        summary['sonne_metric'] = self.create_metric_list('sonne',summary['metrics'])
        summary['regen_metric'] = self.create_metric_list('regen', summary['metrics'])
        summary['luft_metric'] = self.create_metric_list('luft', summary['metrics'])

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
    def get_metric(self,metric_list,start, end,stationnummer):

        proj = {'_id': 0, '_job': 0, '_metric': 0, '_src': 0, }

        if 'regen' in metric_list:
            query = {'stations_id':stationnummer,'_metric': 'regen', 'mess_datum': {'$gte': start, '$lt': end}}
            cur = self.metric_collection.find(query, proj)
            df_regen = pd.DataFrame(list(cur))
            df_regen.rename(columns={'qualitaets_niveau': 'qualitaets_niveau_regen'}, inplace=True)
            df_final = df_regen
            df_final.head()

        if 'sonne' in metric_list:
            sonne_id_list = self.metric_collection.distinct(u'_id',
                                            {'_metric': 'sonne', 'mess_datum': {'$gte': start, '$lt': end}})
            query = {u'_id': {'$in': sonne_id_list}}
            cur = self.metric_collection.find(query, proj)
            df_sonne = pd.DataFrame(list(cur))
            df_sonne.rename(columns={'struktur_version': 'struktur_version_sonne'}, inplace=True)
            df_sonne.rename(columns={'qualitaets_niveau': 'qualitaets_niveau_sonne'}, inplace=True)
            df_sonne.head()

            if 'regen' in metric_list:
                df_final = pd.merge(df_sonne, df_final, on=['stations_id', 'mess_datum'], how='inner',
                                    suffixes=['_sonne', '_regen'])
            else:
                df_final = df_sonne
                #df_final.insert(0,'qualitaets_niveau_regen',np.nan)


        if 'luft' in metric_list:
            luft_id_list = self.metric_collection.distinct(u'_id',
                                           {'_metric': 'luft', 'mess_datum': {'$gte': start, '$lt': end}})
            query = {u'_id': {'$in': luft_id_list}}
            cur = self.metric_collection.find(query, proj)
            df_luft = pd.DataFrame(list(cur))
            df_luft.rename(columns={'struktur_version': 'struktur_version_luft'}, inplace=True)
            df_luft.rename(columns={'qualitaets_niveau': 'qualitaets_niveau_luft'}, inplace=True)
            df_luft.head()

            if ('sonne' in metric_list or 'regen' in metric_list):
                df_final = pd.merge(df_luft, df_final, on=['stations_id', 'mess_datum'], how='inner',
                                    suffixes=['_luft', ''])
            else:
                df_final = df_luft

        df_final.head()
        return df_final



    def retrieve(self, start, end):
        # Get stations with geo-information
        station_df = self.get_station_geo(start, end)
        station_list = list(set([e for e in station_df.stationsnummer.unique()]))
        metrics = self.get_observed_metric(station_list, start, end)
        df = station_df.merge(metrics, how='left', on=['stationsnummer'])
        df.rename(columns={'stationsnummer': 'station_id'}, inplace=True)


        # TODO: Add the metrics filter-list here [....]
        return df


wr = WeatherReport()
stations = wr.retrieve(start=datetime.datetime(2016, 1, 1),
                       end=datetime.datetime(2016, 2, 1))

print(stations)
stations.to_csv("/tmp/WeatherApp_results___V01.csv", sep='\t',encoding='utf-8')

