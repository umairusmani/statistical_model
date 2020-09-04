#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from datetime import timedelta  
import numpy as np
from datetime import datetime
import operator
from collections import defaultdict
import os
import xlsxwriter
import more_itertools as mit
import math
import time

#  ---------------------- Added by NM on Aug, 31, 2020 ----------

#  Adding DB call using databas.ini and conf.py

import sys
# from config import config
from datetime import date, datetime
# import psycopg2
# import psycopg2.extras
import time
from functools import singledispatch
from os import listdir


# In[2]:


# for making connection with sqlite db
import sqlite3
def config():
    try:
        conn = sqlite3.connect('public.db')
        return conn
    except:
        return None


# In[3]:


#-------- Connect into DB and Select data from DBA --hs_pipeline table
def db_conn():
    conn = None
    try:
        conn_parameters = config()
        conn = psycopg2.connect(**conn_parameters)
        print(conn)
        cur = conn.cursor()
    except (Exception, psycopg2.Error) as error :
        print("Not able to connect into the Database {}\n".format(error)) 
    finally:
        if conn is not None:
            print(" DB is connected!\n")

    get_pid_id="0001_0001"
    hs_table_pipleline = "hs_16_clean_pipeline_SC_"
    hs_table = ''.join(hs_table_pipleline+get_pid_id) #hs_16_clean_pipeline_SC_0001_0001

    # Let us Select the data from the DB pipeline  and see what would happen

    hs_fetch_data_statement = "select * from public.\"%s\" ORDER BY timestamp DESC limit 1;" %(hs_table)
    print("We are selecting data from Humidity pipleine table:\n", hs_fetch_data_statement)
    dh_stat = pd.read_sql_query(hs_fetch_data_statement, con=conn)

    df_stat_clean = list(dh_stat.values)
    print(df_stat_clean)
    
    data_dh = pd.DataFrame(df_stat_clean[:1])

    dh_stat_numpy = np.array(data_dh.iloc[:,3:243])
    dh_stat_timestamp = np.array(data_dh.iloc[:,2:3], dtype=str)
      
    #print(dh_stat_numpy)
    
    return dh_stat_numpy, dh_stat_timestamp

# Call Humidity DB connec & Select data from DBA Humidity pipeline table


# In[4]:


# # Get start time of each interval
def get_intervals(time_overlap, data):
    
    start_time = []
    
    file_start_time = data["timestamp"][0]
    
    file_end_time = data["timestamp"][len(data.values)-1]
    
    start_time.append(file_start_time)
    
    while start_time[-1] + timedelta(seconds=time_overlap*60) <= file_end_time:
        
        start_time.append(start_time[-1] + timedelta(seconds=time_overlap*60))
        
    return start_time


# In[5]:


def get_records(start_time, end_time, data, sensor_columns):
    
    #greater than the start date and smaller than the end date
    mask = (data['timestamp'] > start_time) & (data['timestamp'] <= end_time)
    
    return data.loc[mask][sensor_columns]


# In[6]:


# # Get average change of top 6 sensors
def get_avg_change_top_6(data):
    
    change_in_sensors = {}
    
    # calculate average change in each sensor of center zone 
    for column in data.keys():
    
        change = [(abs(b - a)/(a+0.1)) for a, b in zip(data[column][1:].astype('float64'),data[column][2:].astype('float64'))]
    
        change_in_sensors[column] = np.mean(change)
        
    # get top 6 sensors
    top_6 = dict(sorted(change_in_sensors.items(), key=operator.itemgetter(1),reverse=True)[:6])
    
    # get average change of top 6 sensors
    avg_change = 0

    for key in top_6:
    
        avg_change += top_6[key]
    
    avg_change = avg_change/float(6)
    
    return avg_change

  # Top 6 sensors means the six sensors that will have maximum change 


# In[7]:


# # Get percent change from last interval
def get_percent_change(current_record, previous_record):
    
    percent_change_in_sensors = {}
    
    # calculate percent change in each 16 perimeter reference sensor 
    for column in current_record.keys():
    
        percent_change = [(abs(a - b)/(b+0.1))*100.0 for a, b in zip(current_record[column][1:].astype('float64'), previous_record[column][1:].astype('float64'))]
    
        percent_change_in_sensors[column] = np.mean(percent_change)
        
    # get top 6 sensors
    top_6 = dict(sorted(percent_change_in_sensors.items(), key=operator.itemgetter(1),reverse=True)[:6])
    
    # get average change of top 6 sensors
    percent_avg_change = 0

    for key in top_6:
    
        percent_avg_change += top_6[key]
    
    percent_avg_change = percent_avg_change/float(6)
    
    return percent_avg_change   


# In[8]:


# # Function for extracting required timestamps and labels
def extract_data(timeslots, actual_labels, ts):
    
    timestamps = []
    
    labels = []
    
    flag = 0
    
    convert_timestamp = ''
    
    interval = 0
    
    for k in timeslots.keys()[1:]:
        
        # if date or time is NULL
        if type(timeslots[k][ts]) == float and math.isnan(float(timeslots[k][ts])):
            
            # if date is NULL, no need to save time
            if "Date" in k:
            
                flag = -1
                
            # if date is available but time is NULL just empty the converted timestamp
            else:
                
                convert_timestamp = ''
        else:
                    
            # get time
            if "Assessment" in k:
        
                time = timeslots[k][ts]
            
                convert_timestamp += str(time)
                
                if flag >= 0:
            
                    # Convert string to datetime object
                    converted_timestamp = datetime.strptime(convert_timestamp, '%Y-%m-%d %H:%M:%S')
        
                    # Save all converted timestamps and labels
            
                    if actual_labels["T"+str(interval+1)][ts] == "dry":
                    
                        labels.append(0)
            
                        timestamps.append(converted_timestamp)
                
                    elif actual_labels["T"+str(interval+1)][ts] == "wet":
                        
                        labels.append(1)
                        
                        timestamps.append(converted_timestamp)
                        
            
                    convert_timestamp = ''
                
                    flag = 0
                    
                else:
                    
                    flag = 0
            
                interval += 1
            
            # get date
            else:
            
                # if year is first in the date
                if len(str(timeslots[k][ts]).split("-")[0]) > 2:
                
                    convert_timestamp = str(timeslots[k][ts]).split("-")[0] + "-" + str(timeslots[k][ts]).split("-")[2].split(" ")[0] + "-" + str(timeslots[k][ts]).split("-")[1] + " " 
                
                # if year is not first in the date
                else:
            
                    date = str(timeslots[k][ts])
                
                    convert_timestamp = date.split("-")[2] + "-" + date.split("-")[1] + "-" + date.split("-")[0] + " "
             
                
    return timestamps, labels 


# In[9]:


def stats(time_window, time_overlap, database_name , sensor_columns, threshold, percent_change):

    #table_names  = get table names
    
    #e.g. table_names = [hs_16_clean_pipeline_SC_0001_0001]
    
#     executing loop for each table(for each patients)
    for table in table_names:
        
       
        conn = config()
        
        #data  = "select * query from hs_16_clean_pipeline_SC_0001_0001"
        data = pd.read_sql("select * from "+table,conn)
        # get patient id
        pid = table.split( "ref_val_16_SC_" )[-1]
        
        # convert timestamp from string to datetime
        data["timestamp"] = pd.to_datetime(data['timestamp'], format='%Y-%m-%d %H:%M:%S') 
        
        interfaceid = data.iloc[0]["interfaceid"] 
        gatewayid = data.iloc[0]["gatewayid"] 
        
        # get intervals in file
        intervals_start = get_intervals(time_overlap, data)
        
        intervals = 0 # number of intervals processed
        saved_intervals = [] # saved processed intervals
        patient = {} # dictionary for creating a dataframe for each patient
        
        patient["Patient ID"] = pid
#         list for dumbing record in db table "cur_app_clinical_events"
        dump_db = [  ]
        
        
        for start_time in intervals_start:
            
            # get records of center zone sensors for each interval
            records = get_records(start_time, start_time + timedelta(seconds=time_window*60), data, sensor_columns)
            
            patient["Interval Time Start"] = start_time
            
            patient["Interval Time End"] = start_time + timedelta(seconds=time_window*60)
            
            # get average change of top 6 sensors
            avg_change = get_avg_change_top_6(records)
            
            patient["Avg. change of top6"] = avg_change
            
            # get percent change from last interval
            if intervals == 0:
                
                percent_change_last_interval = 0
                
                patient["Percent Change from last interval"] = percent_change_last_interval
                
                patient["flag"] = 0
                
                df = pd.DataFrame([patient], columns = ['Patient ID', 'Interval Time Start', 'Interval Time End', 'Avg. change of top6', 'Percent Change from last interval', 'flag'])
                
                print("columns",patient.keys())
                
                dump_db.append( [ interfaceid , gatewayid ,  start_time  , patient["Percent Change from last interval"]  , patient["flag"] ] )
                
                if os.path.isdir('./data/tw_'+str(time_window) + "_to_" + str(time_overlap) + "_th_" + str(threshold)+ "_pc_" + str(percent_change)):

                    df.to_csv('./data/tw_'+str(time_window) + "_to_" + str(time_overlap)+ "_th_" + str(threshold)+ "_pc_" + str(percent_change)+"/"+pid+"_Results"+'.csv', mode='w', header=True)

                else:

                    os.makedirs('./data/tw_'+str(time_window) + "_to_" + str(time_overlap)+ "_th_" + str(threshold)+ "_pc_" + str(percent_change))
                    
                    df.to_csv('./data/tw_'+str(time_window) + "_to_" + str(time_overlap)+ "_th_" + str(threshold)+ "_pc_" + str(percent_change) + "/"+pid+"_Results"+'.csv', mode='w', header=True)
    
            else:
                
                percent_change_last_interval = get_percent_change(records, saved_intervals[-1])
                
                patient["Percent Change from last interval"] = percent_change_last_interval
                
                # flag row if percent change from last interval is greater than 70
                if percent_change_last_interval > percent_change:
                    
                    patient["flag"] = 1
                    
                else:
                    
                    patient["flag"] = 0
                
                
                
                dump_db.append( [ interfaceid , gatewayid ,  start_time  , patient["Percent Change from last interval"]  , patient["flag"] ] )
                
                
                df = pd.DataFrame([patient], columns = ['Patient ID', 'Interval Time Start', 'Interval Time End', 'Avg. change of top6', 'Percent Change from last interval', 'flag'])
                    
                df.to_csv('./data/tw_'+str(time_window) + "_to_" + str(time_overlap)+ "_th_" + str(threshold)+ "_pc_" + str(percent_change)+"/"+pid+"_Results"+'.csv', mode='a', header=False)
                    
            
            
                
            intervals += 1
            
            saved_intervals.append(records)
            
#         print( dump_db )
#       dumping result records in db table "cur_app_clinical_events"
        pd.DataFrame(dump_db,columns=["interfaceid","gatewayid","timestamp","percent_change","event_type"]).to_sql("cur_app_clinical_events",config(),if_exists='replace')


# In[10]:


# Function for validation and table creation
import sqlparse
def create_table(path="../Data Model & Schema/Database-A.sql"):
    cur = None
    table_str = ""
    with open(path) as schema_file:
        create_table = schema_file.readlines()
        validation_string = "".join(create_table).replace('"','')
        try:
            validation_string = sqlparse.parse(validation_string)
        except Exception as e:
            print("Bad Statement : ",e )
        try:
            conn = config()
        except:
            return cur
        cur = conn.cursor()
        for table in validation_string:
            try:
                print(type(table))
                cur.execute(str(table))
            except Exception as e:
                print("error in creating table")
                print(e)
    return cur
    


# In[11]:


# function for dumping json data into tables
def dump_data():
    center_sensor = pd.read_json("../JSON/_hs_16_clean_SC_0001_0001__202008281500.json")
    ref_sensor = pd.read_json("../JSON/_ref_val_16_SC_0001_0001__202008281501.json")
    ref_sensor = pd.DataFrame(ref_sensor.ref_val_16_SC_0001_0001.to_list())
    center_sensor = pd.DataFrame(center_sensor.hs_16_clean_pipeline_SC_0001_0001.to_list())
    ref_sensor = ref_sensor.drop(["RefT_1","RefT_2","RefT_3","RefT_4","RefT_5","RefT_6","RefT_7","RefT_8",
                                  "RefT_9","RefT_10","RefT_11","RefT_12","RefT_13","RefT_14","RefT_15","RefT_16"]
                                  ,axis=1)
    conn = config()
    ref_sensor.to_sql("ref_val_16_SC_0001_0001",conn,if_exists='replace')
    center_sensor.to_sql("hs_16_clean_pipeline_SC_0001_0001",conn,if_exists='replace')


# In[ ]:





# In[12]:


# creating tables in database for Gateway Input
create_table("../Data Model & Schema/Database-A.sql")
create_table("../Data Model & Schema/Database-C.sql")
dump_data()


# In[13]:


#'CenterZone sensors'
sensor_columns = [ "H_93","H_103","H_113","H_123","H_133","H_143",
                   "H_94","H_104","H_114","H_124","H_134","H_144",
                   "H_95","H_105","H_115","H_125","H_135","H_145",
                   "H_96","H_106","H_116","H_126","H_136","H_146",
                   "H_97","H_107","H_117","H_127","H_137","H_147",
                   "H_98","H_108","H_118","H_128","H_138","H_148" ]
# reference_sensors
ref_columns = ['RefH_1', 'RefH_2','RefH_3', 'RefH_4', 'RefH_5', 'RefH_6', 'RefH_7', 'RefH_8', 'RefH_9',
               'RefH_10', 'RefH_11', 'RefH_12', 'RefH_13', 'RefH_14', 'RefH_15','RefH_16']

# # Parameter Tuning

# In[ ]:


time_window = 15      # difference of End and Start timestamp in a event

time_overlap = 1     # difference among the start timestamp of two consecutive rows

threshold = 10        # criteria of picking up the consecutive number of rows

percent_change = 1   # percentage change for an interval

performance = {} # for storing accuracies


# In[14]:


table_names = ["ref_val_16_SC_0001_0001"]
# perform statistical analysis on all the files in the given tables
stats(time_window, time_overlap, table_names, ref_columns, threshold, percent_change)


# In[15]:


print("its done")


# In[ ]:


conn = 

