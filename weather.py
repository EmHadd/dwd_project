# coding=utf-8
import datetime
from pymongo import MongoClient
import pandas as pd
import numpy as np

import core.queue.job

mongo_url = 'mongodb://core3:654321@localhost:27017'



class WeatherReport():


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


    def retrieveMetrics(self,join_how,start,end):
        #define metrics columns
        insert_columns_regen = ['niederschlag_gefallen_ind', 'niederschlagsform', 'niederschlagshoehe',
                                'qualitaets_niveau_regen']
        insert_columns_sonne = ['qualitaets_niveau_sonne', 'struktur_version_sonne', 'stundensumme_sonnenschein']
        insert_columns_luft = ['lufttemperatur', 'qualitaets_niveau_luft', 'rel_feuchte', 'struktur_version_luft']
        metric_list = ['luft', 'sonne', 'regen']
        join_how = 'outer'
        proj = {'_id': 0, '_job': 0, '_metric': 0, '_src': 0, }
        #if metric list contains regen, retrieve regen data from the mongo dwd
        if 'regen' in metric_list:
            regen_id_list = self.metric_collection.distinct(u'_id',
                                            {'_metric': 'regen', 'mess_datum': {'$gte': start, '$lt': end}})
            query = {u'_id': {'$in': regen_id_list}}
            cur = self.metric_collection.find(query, proj)
            df_regen = pd.DataFrame(list(cur))
            #if the retrieved cursor not empty, amend some columns names
            if not df_regen.empty:
                df_regen.rename(columns={'qualitaets_niveau': 'qualitaets_niveau_regen'}, inplace=True)
                df_regen = df_regen[['stations_id', 'mess_datum'] + insert_columns_regen]
                df_regen.head()

        else: #if metric list doesn't contain regen, create an empty data frame

            df_regen = pd.DataFrame()
        if 'sonne' in metric_list:
            sonne_id_list = self.metric_collection.distinct(u'_id',
                                            {'_metric': 'sonne', 'mess_datum': {'$gte': start, '$lt': end}})
            query = {u'_id': {'$in': sonne_id_list}}
            cur = self.metric_collection.find(query, proj)
            df_sonne = pd.DataFrame(list(cur))

            if not df_sonne.empty:
                df_sonne.rename(columns={'struktur_version': 'struktur_version_sonne'}, inplace=True)
                df_sonne.rename(columns={'qualitaets_niveau': 'qualitaets_niveau_sonne'}, inplace=True)
                df_sonne = df_sonne[['stations_id', 'mess_datum'] + insert_columns_sonne]
                df_sonne.head()

        else:

            df_sonne = pd.DataFrame()
        if 'luft' in metric_list:
            luft_id_list = self.metric_collection.distinct(u'_id',
                                           {'_metric': 'luft', 'mess_datum': {'$gte': start, '$lt': end}})
            query = {u'_id': {'$in': luft_id_list}}
            cur = self.metric_collection.find(query, proj)
            df_luft = pd.DataFrame(list(cur))

            if not df_luft.empty:
                df_luft.rename(columns={'struktur_version': 'struktur_version_luft'}, inplace=True)
                df_luft.rename(columns={'qualitaets_niveau': 'qualitaets_niveau_luft'}, inplace=True)
                df_luft = df_luft[['stations_id', 'mess_datum'] + insert_columns_luft]
                df_luft.head()

        else:

            df_luft = pd.DataFrame()

        df_list = [df_regen, df_sonne, df_luft]

        df_list_not_empty = []

        for df in df_list:
            if not df.empty:
                df_list_not_empty = df_list_not_empty[:] + [df]

        if len(df_list_not_empty) == 0:
            df_final = pd.DataFrame()
        elif len(df_list_not_empty) == 1:
            df_final = df_list_not_empty[0]
        elif len(df_list_not_empty) >= 2:

            df_final = pd.merge(df_list_not_empty[0], df_list_not_empty[1], on=['stations_id', 'mess_datum'],
                                how=join_how, suffixes=['', ''])

        if len(df_list_not_empty) == 3:
            df_final = pd.merge(df_final, df_list_not_empty[2], on=['stations_id', 'mess_datum'], how=join_how,
                                suffixes=['', ''])

        if 'sonne' not in metric_list:
            for col in insert_columns_sonne:
                df_final[col] = np.nan

        if 'luft' not in metric_list:
            for col in insert_columns_luft:
                df_final[col] = np.nan

        if 'regen' not in metric_list:
            for col in insert_columns_regen:
                df_final[col] = np.nan

        df_final = df_final[
            ['stations_id', 'mess_datum'] + insert_columns_sonne + insert_columns_regen + insert_columns_luft]
        df_final.rename(columns={'stations_id': 'stationsnummer'}, inplace=True)
        return df_final


    def retrieve(self, start, end):  # , zip=[], rain=True, sun=True, air=True):
        # Get stations with geo-information
        station_df = self.get_station_geo(start, end)
        station_list = list(set([e for e in station_df.stationsnummer.unique()]))
        #get type of mesured metric
        metrics = self.get_observed_metric(station_list, start, end)
        #merge stations info with measured metrics
        df = station_df.merge(metrics, how='left', on=['stationsnummer'])
        #retrieve measured metrics for each station in the given time
        df_final = self.retrieveMetrics("outer",start,end)
        #final merge of the stations info with the measured metrics... this outcome is passed for data aggregation
        finDf = df.merge(df_final, how='left', on=['stationsnummer'])


        return finDf


wr = WeatherReport()
stations = wr.retrieve(start=datetime.datetime(2016, 1, 1), end=datetime.datetime(2016, 1, 10))

#print(stations.shape)
#print(stations.head())
stations.to_csv("/media/sf_Haddie/Documents/python/WeatherApp_allResults___V01.csv", sep='\t',encoding='utf-8')
#fn = wr.retrieveMetrics( 'outer',start=datetime.datetime(2016, 1, 1),
#                       end=datetime.datetime(2016, 1, 10))
#print(fn.head())
#fn.to_csv("/media/sf_Haddie/Documents/python/metrics_results___V01.csv", sep='\t',encoding='utf-8')


