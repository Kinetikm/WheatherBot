import pandas as pd
import requests
from datetime import date, timedelta
import json
from sqlalchemy import create_engine
import os
import sys
import traceback
from argparse import ArgumentParser
import numpy as np


def get_condition_code(condition):
    """
    The function return condition code using it's string value
    :param condition: String, condition name
    :return: int, condition code

    clean_ar - array of conditions, which are equal clean sky
    partly_cloudy_arr - array of conditions, which are equal partly cloudy sky
    cloudy_arr - array of conditions, which are equal cloudy sky
    prec_clouds_arr - array of conditions, which are equal precipitation clouds
    other_sky_arr - array of unknown conditions

    codes of sky_state:
    1 - clean_sky
    2 - partly_cloudy_sky
    3 - cloudy_sky
    4 - precipitation_cloudy_sky
    0 - other_sky
    404 - error code
    """
    clean_arr = ['Clear', 'Haze', 'Mostly Sunny', 'Sunny']
    partly_cloudy_arr = ['Scattered Clouds', 'Partly Sunny', 'Partly Cloudy']
    cloudy_arr = ['Overcast', 'Cloudy', 'Mostly Cloudy']
    prec_clouds_arr = ['Chance of Flurries', 'Chance of Rain', 'Chance Rain',
                       'Chance of Freezing Rain', 'Chance of Sleet', 'Chance of Snow',
                       'Chance of Thunderstorms', 'Chance of a Thunderstorm',
                       'Flurries', 'Freezing Rain', 'Rain', 'Sleet', 'Snow',
                       'Thunderstorms', 'Thunderstorm', 'Light Freezing Rain', 'Light Freezing Drizzle',
                       'Light Drizzle']
    other_sky_arr = ['Fog', 'Unknown', 'Overcast']
    if condition in clean_arr:
        return 1
    elif condition in partly_cloudy_arr:
        return 2
    elif condition in cloudy_arr:
        return 3
    elif condition in prec_clouds_arr:
        return 4
    elif condition in other_sky_arr:
        return 0
    else:
        return None


def get_whether_forecast_one_day(city_name, city_id, api_key):
    """
    The function gets one day forecast throw wundergroud api
    :param city_name: string, which contains city name
    :param city_id: int, city id for insert in table
    :param api_key: string, which contains api_key for access
    :return: dataframe with oneday weather forecast
    """

    url = "http://api.wunderground.com/api/" + api_key + "/hourly/q/Russia/" + city_name + ".json"
    try:
        forecast_main = json.loads(requests.get(url).text)['hourly_forecast']
        wh_forecast_data = pd.DataFrame([], columns=['city_id', 'datetime', 'temperature', 'pressure',
                                                     'wind', 'precipitation_prob', 'sky', 'humidity'])

        for i in range(len(forecast_main)):
            child = forecast_main[i]['FCTTIME']
            temperature = int(forecast_main[i]['temp']['metric'])
            humidity = int(forecast_main[i]['humidity'])
            precipitation_prob = float(forecast_main[i]['pop'])/100
            wind = float(forecast_main[i]['wspd']['metric'])/3.6
            sky_condition = get_condition_code(forecast_main[i]['condition'])
            pressure = 0
            wh_forecast_data.loc[len(wh_forecast_data)] = [city_id,
                                                           pd.to_datetime(child['year'] + child['mon'] +
                                                                          child['mday'] + child['hour'] + child['min'],
                                                                          format='%Y%m%d%H%M'),
                                                           pressure, temperature, wind, precipitation_prob,
                                                           sky_condition, humidity]
        return wh_forecast_data
    except Exception as e:
        print(traceback.format_exc())
        print(e)


def get_whether_forecast_10_days(city_name, city_id, api_key):
    """
    The function gets 10 days forecast throw wundergroud api
    :param city_name: string, which contains city name
    :param city_id: int, city id for insert in table
    :param api_key: string, which contains api_key for access
    :return: dataframe with oneday weather forecast
    """

    url = "http://api.wunderground.com/api/"+api_key+"/hourly10day/q/Russia/"+city_name+".json"
    try:
        forecast_main = json.loads(requests.get(url).text)['hourly_forecast']
        wh_forecast10_data = pd.DataFrame([], columns=['city_id', 'datetime', 'temperature', 'pressure',
                                                       'wind', 'precipitation_prob', 'sky', 'humidity'])
        for i in range(len(forecast_main)):
            child = forecast_main[i]['FCTTIME']
            temperature = int(forecast_main[i]['temp']['metric'])
            humidity = int(forecast_main[i]['humidity'])
            pressure = 0
            sky_condition = get_condition_code(forecast_main[i]['condition'])
            precipitation_prob = float(forecast_main[i]['pop'])/100
            wind = float(forecast_main[i]['wspd']['metric'])/3.6
            wh_forecast10_data.loc[len(wh_forecast10_data)] = [city_id,
                                                               pd.to_datetime(child['year'] + child['mon'] +
                                                                              child['mday'] + child['hour'] +
                                                                              child['min'], format='%Y%m%d%H%M'),
                                                               pressure, temperature, wind, precipitation_prob,
                                                               sky_condition, humidity]
        return wh_forecast10_data
    except Exception as e:
        print(traceback.format_exc())
        print(e)


def get_whether_history(city_name, city_id, api_key, date_for_history=None, skipperiod=1):
    """
    The function gets one day history throw wundergroud api
    :param city_name: string, which contains name of city for getting data
    :param city_id: int, city id for insert in table
    :param api_key: api_key, for connect with api
    :param date_for_history: string, date in format 'YYYY-mm-dd'
    :param skipperiod: int, day from today count in back. For example, skipperiod =1 -> yesterday history
    :return: Dataframe with hourly history for given day
    """

    if date_for_history is None:
        day_for_data = str(date.today() - timedelta(skipperiod))
    else:
        day_for_data = date_for_history
    url = "http://api.wunderground.com/api/" + api_key + "/history_" + day_for_data[0:4] + day_for_data[5:7] + \
          day_for_data[8:10] + "/q/Russia/" + city_name + ".json"
    try:
        history_main = json.loads(requests.get(url).text)
        wh_history_data = pd.DataFrame(columns=['city_id', 'datetime', 'temperature', 'pressure',
                                                'wind', 'precipitation', 'sky_state', 'humidity'])
        observation_data = history_main['history']['observations']
        for i in range(len(observation_data)):
            obj = observation_data[i]
            temperature = float(obj['tempm'])
            humidity = float(obj['hum'])
            sky_condition = get_condition_code(obj['conds'])
            wind = float(obj['wspdm'])/3.6
            pressure = float(obj['pressurem'])*0.75
            if int(obj['rain']) != 0 or int(obj['snow']) != 0:
                precipitation = 1
            else:
                precipitation = 0
            wh_history_data.loc[len(wh_history_data)] = [city_id,
                                                         pd.to_datetime(day_for_data.replace('-', '')[0:8] +
                                                                        obj['date']['hour'] + obj['date']['min'],
                                                                        format='%Y%m%d%H%M'),
                                                         temperature, pressure, wind, precipitation, sky_condition,
                                                         humidity]
        return wh_history_data
    except Exception as e:
        print(traceback.format_exc())
        print(e)


def get_multiple_days_history(city_name, city_id, api_key, start_time=None, end_time=None):
    """
    The function may be called for get weather history for specific city for period
    :param city_name: string, city name
    :param city_id: int, city id for insert in table
    :param api_key: api_key, for connect with api
    :param start_date: string, start point for getting history in format "YYYYmmdd""
    :param end_date: string, last point for getting history, no more than date.today() in format "YYYYmmdd"
    :return:
    """
    try:
        start_date = pd.to_datetime(start_time, format="%Y%m%d")
        end_date = pd.to_datetime(end_time, format="%Y%m%d")
        wrk_date = start_date
        wh_history_data = pd.DataFrame(columns=['city_id', 'datetime', 'temperature', 'pressure',
                                                'wind', 'precipitation', 'sky_state', 'humidity'])
        while wrk_date != end_date:
            wh_history_data = pd.concat([wh_history_data, get_whether_history(city_name, city_id, api_key,
                                                                              str(wrk_date))])
            wrk_date = wrk_date + timedelta(1)
        return wh_history_data
    except Exception as ex:
        print(traceback.format_exc())
        print(ex)


def idempotent_upload(df, target_table, task_keys, conn_string,
                      n_trials=5, quoting=None, null_repr=None):
    """
    A Python implementation of "upsert".

    Uploads data from `df` to `target_table`
    in Postgres in an idempotent fashion
    (an operator is called idempotent if its
    double application equals its single
    application: I(I(x)) = I(x) for all x).

    Idempotency is obtained by deleting all rows
    that should be inserted. Such rows are found
    by their values of columns from `task_keys`.
    Each element of list `task_keys` is a tuple
    (column_name, column_sql_type). This approach
    is chosen, because now in our databases
    column named 'date' can be both date and
    timestamp.

    Note that idempotency is guaranteed until
    `df` is not changed. If `df` is a result
    of process depending on external data,
    even number of inserted rows can differ
    from number of deleted rows.

    :param df: pandas::DataFrame
    :param target_table: string
    :param task_keys: list of tuples
    :param conn_string: string
    :param n_trials: integer
    :param quoting: csv::flag
    :param null_repr: any
    :return: None
    """
    # TODO: Can we use ON CONFLICT UPDATE instead of manual deletion?
    # TODO: missed value of type `double` -> failure without exception
    to_be_deleted = df[[x[0] for x in task_keys]].drop_duplicates()
    del_stream = StringIO()
    del_stream.write(to_be_deleted.to_csv(sep='\t', encoding='utf8',
                                          index=False, header=False))
    del_stream.seek(0)

    stream = StringIO()
    stream.write(df.to_csv(sep='\t', encoding='utf8',
                           index=False, header=False, quoting=quoting))
    stream.seek(0)

    for i in range(n_trials):
        conn = None
        curs = None
        try:
            conn = psycopg2.connect(conn_string)
            curs = conn.cursor()
            curs.execute(
                '''
                CREATE TEMP TABLE to_be_deleted
                (
                    {}
                )
                '''.format(',\n'.join(['{} {}'.format(*x) for x in task_keys]))
            )
            # No `null_repr`, because keys must not be NULL.
            curs.copy_from(del_stream, 'to_be_deleted')
            curs.execute(
                '''
                DELETE FROM
                    {0}
                WHERE
                    ({1}) IN (SELECT {1} FROM to_be_deleted)
                '''.format(target_table,
                           ', '.join([x[0] for x in task_keys]))
            )
            if null_repr is not None:
                curs.copy_from(stream, target_table, null=null_repr)
            else:
                curs.copy_from(stream, target_table)
            conn.commit()
        except Exception as e:
            if i == n_trials - 1:
                msg = "idempotent_upload failed:\n" + str(e)
                send_slack_message(msg)
                raise e
            else:
                print(e)
                time.sleep(60 * (i + 1))
        else:
            break
        finally:
            if curs is not None:
                curs.close()
            if conn is not None:
                conn.close()


def get_day_for_def_whether(disk_engine,tablename, city_id):
    """
    The function gets the latest history saved data by region_id.
    It returns skipperiod from today data
    params:
        disk_engine - configuration for sql database
        region_id - integer of a region
    """

    try:
        start_date = pd.read_sql('''
            SELECT max(datetime) date
            FROM
                {}
            WHERE
                city_id={}
                '''.format(tablename, city_id), disk_engine)['date'][0]
        return (date.today()-start_date.date()).days
    except Exception as ex:
        print(traceback.format_exc())
        print(ex)


def main():
    parser = ArgumentParser()
    parser.add_argument('-f', '--forecat_true', dest='forecast_getter', action='store_true', help='activate forecast getter')
    parser.set_defaults(forecast_getter=False)
    parser.add_argument('-l', '--long_forecast', dest='long_forecast', action='store_true', help='get forecast for 10 days')
    parser.set_defaults(long_forecast=False)
    parser.add_argument('-ht', '--history_true', dest='history_getter', action='store_true', help='activate history getter')
    parser.set_defaults(history_getter=False)
    parser.add_argument('-db', '--history_on_database', dest='base_history_getter', action='store_true',
                        help='get history data based on DB last value')
    parser.set_defaults(base_history_getter=False)
    parser.add_argument('-a', '--api_key', type=str, default='c4d5d9d99bd4ed54', help='Api_key for getting data')
    parser.add_argument('-s', '--start_date', type=str, default=None, help='Start date for getting history in format'
                                                                            'YYmmdd')
    parser.add_argument('-e', '--end_date', type=str, default=date.today(), help='End date for getting history in'
                                                                                  ' format YYmmdd')

    cli_args = parser.parse_args()

    cities = {"Moscow": 102, "Saint_Petersburg": 104}
    api_key = cli_args.api_key
    conn_string = ''
    tablename_for_history=''
    tablename_for_forecast=''

    if cli_args.forecast_getter:
        try:
            if cli_args.long_forecast:
                for city in cities:
                    forecast_data = get_whether_forecast_10_days(city, cities[city], api_key)
                    forecast_data['city_id'] = forecast_data['city_id'].apply(int)
                    forecast_data.drop_duplicates(subset=['city_id', 'datetime'], inplace=True)
                    idempotent_upload(forecast_data, tablename_for_forecast,
                                      [('city_id', 'int'), ('datetime', 'timestamp')],
                                      conn_string, null_repr='')
                    print("Forecast is inserted for city with id = {}".format(cities[city]))
            else:
                for city in cities:
                    forecast_data = get_whether_forecast_one_day(city, cities[city], api_key)
                    forecast_data['city_id'] = forecast_data['city_id'].apply(int)
                    forecast_data.drop_duplicates(subset=['city_id', 'datetime'], inplace=True)
                    idempotent_upload(forecast_data, tablename_for_forecast, [('city_id', 'int'), ('datetime', 'timestamp')],
                                      conn_string, null_repr='')
                    print("Forecast is inserted for city with id = {}".format(cities[city]))
        except Exception as ex:
            print traceback.format_exc()
            print(ex)

    if cli_args.history_getter:
        try:
            disk_engine = create_engine(conn_string)
            if cli_args.base_history_getter:
                for city in cities:
                    skippedperiod = get_day_for_def_whether(disk_engine, tablename_for_history, cities[city])
                    for i in range(1, skippedperiod + 1):
                        history_data = get_whether_history(city, cities[city], api_key, skipperiod=i)
                        history_data['city_id'] = history_data['city_id'].apply(int)
                        history_data.drop_duplicates(subset=['city_id', 'datetime'], inplace=True)
                        idempotent_upload(history_data, tablename_for_history,
                                          [('city_id', 'int'), ('datetime', 'timestamp')], conn_string, null_repr='')
                    print('Data inserted. City id = {}'.format(cities[city]))
            elif cli_args.start_date is not None and cli_args.end_date is not None:
                if cli_args.start_date == cli_args.end_date:
                    for city in cities:
                        history_data = get_whether_history(city, cities[city], api_key,
                                                           date_for_history=cli_args.start_date)
                        history_data['city_id'] = history_data['city_id'].apply(int)
                        history_data.drop_duplicates(subset=['city_id', 'datetime'], inplace=True)
                        idempotent_upload(history_data, tablename_for_history,
                                          [('city_id', 'int'), ('datetime', 'timestamp')],
                                          conn_string, null_repr='')
                        print('Data inserted. City id = {}'.format(cities[city]))
                else:
                    for city in cities:
                        history_data = get_multiple_days_history(city, cities[city], api_key, cli_args.start_date, cli_args.end_date)
                        history_data['city_id'] = history_data['city_id'].apply(int)
                        history_data.drop_duplicates(subset=['city_id', 'datetime'], inplace=True)
                        idempotent_upload(history_data, tablename_for_history,
                                          [('city_id', 'int'), ('datetime', 'timestamp')], conn_string, null_repr='')
                        print('Data inserted. City id = {}'.format(cities[city]))
            else:
                print("Not enough data")
        except Exception as ex:
            print traceback.format_exc()
            print(ex)

if __name__ == '__main__':
    main()
