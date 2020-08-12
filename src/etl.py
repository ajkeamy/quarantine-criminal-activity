import pandas as pd
import os
import json
import glob
import numpy as np
import doctest
import datetime as dt

def get_present_sf():
    cols = ['Incident Date', 'Incident Time', 'Incident Day of Week','Incident Number',
           'Incident Category', 'Incident Description','Police District',
           'Latitude', 'Longitude','point']
    raw_sf_csv = pd.read_csv('https://data.sfgov.org/api/views/wg3w-h783/rows.csv?accessType=DOWNLOAD',parse_dates=[0,1],infer_datetime_format=True, usecols=cols)
    corona_df = raw_sf_csv[
        (raw_sf_csv['Incident Date'] >= pd.to_datetime('02-01-2018')) & (raw_sf_csv['Incident Date'] <= pd.to_datetime('04-01-2018'))|
        (raw_sf_csv['Incident Date'] >= pd.to_datetime('02-01-2019')) & (raw_sf_csv['Incident Date'] <= pd.to_datetime('04-01-2019'))|
        (raw_sf_csv['Incident Date'] >= pd.to_datetime('02-01-2020')) & (raw_sf_csv['Incident Date'] <= pd.to_datetime('04-01-2020'))]
    corona_df = corona_df.rename(columns={'Incident Date': 'Date', 'Incident Time':'Time', 'Incident Day of Week':'DayOfWeek','Incident Number':'IncidentNum',
           'Incident Category': 'Category', 'Incident Description':'Description','Police District':'PoliceDistrict'})
    return corona_df

def get_hist_sf():
    cols = ['IncidntNum', 'Category', 'Descript', 'DayOfWeek', 'Date', 'Time',
       'PdDistrict', 'X', 'Y', 'Location']
    iter_csv = pd.read_csv('https://data.sfgov.org/api/views/tmnf-yvry/rows.csv?accessType=DOWNLOAD', iterator=True, chunksize=1000, parse_dates=[4,5],infer_datetime_format=True, usecols=cols)
    hist_df = pd.concat([chunk[
            (chunk['Date'] >= pd.to_datetime('02-01-2018')) & (chunk['Date'] <= pd.to_datetime('04-01-2018'))|
            (chunk['Date'] >= pd.to_datetime('02-01-2019')) & (chunk['Date'] <= pd.to_datetime('04-01-2019'))|
            (chunk['Date'] >= pd.to_datetime('02-01-2020')) & (chunk['Date'] <= pd.to_datetime('04-01-2020'))] for chunk in iter_csv])
    hist_df = hist_df.rename(columns={'IncidntNum':'IncidentNum', 'Descript': 'Description','PdDistrict': 'PoliceDistrict', 'X':'Latitude', 'Y':'Longitude', 'Location':'point'})
    return hist_df

def get_sf_data():
    hist = get_hist_sf()
    pres = get_present_sf()
    sf_df = pd.concat([hist,pres],ignore_index=True)
    sf_df = sf_df.drop_duplicates(['IncidentNum'])
    return sf_df

def get_nyc_data():
    cols = ['CMPLNT_FR_DT', 'CMPLNT_FR_TM', 'BORO_NM','CMPLNT_NUM',
           'LAW_CAT_CD', 'OFNS_DESC',
           'Latitude', 'Longitude']
    raw_nyc_csv = pd.read_csv('https://data.cityofnewyork.us/api/views/5uac-w243/rows.csv?accessType=DOWNLOAD',parse_dates=[2,3],infer_datetime_format=True, usecols=cols)
    corona_df = raw_nyc_csv[
        (raw_nyc_csv['CMPLNT_FR_DT'] >= pd.to_datetime('02-01-2018')) & (raw_nyc_csv['CMPLNT_FR_DT'] <= pd.to_datetime('04-01-2018'))|
        (raw_nyc_csv['CMPLNT_FR_DT'] >= pd.to_datetime('02-01-2019')) & (raw_nyc_csv['CMPLNT_FR_DT'] <= pd.to_datetime('04-01-2019'))|
        (raw_nyc_csv['CMPLNT_FR_DT'] >= pd.to_datetime('02-01-2020')) & (raw_nyc_csv['CMPLNT_FR_DT'] <= pd.to_datetime('04-01-2020'))]
    corona_df = corona_df.rename(columns={'CMPLNT_FR_DT': 'Date', 'CMPLNT_FR_TM':'Time', 'BORO_NM':'Borough','CMPLNT_NUM':'IncidentNum',
            'OFNS_DESC':'Description','LAW_CAT_CD':'Level_of_offense'})
    return corona_df