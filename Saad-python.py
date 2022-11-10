---
title: "Distributed Data Analysis - data cleaning"
author: "Saad"
date: "25/03/2022"
---

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import dask
import dask.dataframe as dd
import math
import pandas as pd
import time


spark=SparkSession.builder\
.master("local[*]")\
.appName("WordCount")\
.getOrCreate()
sc=spark.sparkContext


# loading dataset with dask
df = dd.read_csv("/home/hadoop/Documents/pollution.csv", dtype={'CO': 'float64',
       'NO2': 'float64',
       'O3': 'float64',
       'PM10': 'float64',
       'PM2.5': 'float64',
       'RAIN': 'float64',
       'SO2': 'float64'})
                
df.head()

# loading dataset using pandas
dataset = pd.read_csv("C:\\Users\\.....location....\\pollution.csv")

# grouped table create by Aleks --  changed values
grouped_df = df.groupby(['station', 'month']).agg({'TEMP': ['mean'], 'PM2.5': ['mean'], 'PM10': ['mean'], 'SO2': ['mean'], 'O3': ['mean'], 'CO': ['mean'], 'PRES': ['mean'], 'DEWP' : ['mean'], 'RAIN' : ['mean'], 'WSPM' : ['mean']}).compute()


# array that contains all the column names which we will be imputing.
cols = ['TEMP', 'PM2.5', 'PM10', 'SO2', 'O3', 'CO', 'PRES', 'DEWP', 'RAIN', 'WSPM']

# just to view the grouped table
grouped_df

# defining function that takes the station, month and column name, to return the mean.
def getmean(station, month, col):
    # using the station and month values from the grouped table, we extract the mean
    mean = grouped_df.loc[(station, month)][col]['mean']
    return mean

# for loop that iterates through a specific column in this case TEMP
# x is index, c is the value in that location
def clean_data(): 
    # just to count time this loop takes to finish, on my pc it takes around 93 seconds but it will vary so please be patient
    program_start = time.time()
    # first iterate thro column names, and make nested loops for the values inside each column.
    for col in cols:
        for x, c in dataset[col].iteritems():
            # check if value is null / NA 
            if pd.isnull(c):
                # if it is null then get station and month
                station = dataset.loc[x, 'station']
                month = dataset.loc[x, 'month']
                # call the function getmean() to get the mean from the grouped table.
                mean = getmean(station, month, col)
                # change value from Null to mean.
                dataset.loc[x, col] = mean

    finish = time.time()
    print("It has been {0} seconds since the loop started".format(finish - program_start))

clean_data()
# testing if it worked.
count = 0;

# this is only to check for NA values, you can run it before the algorithm, if you run it after it should return 0 for all the columns.


def check_NA(colName):
    count = 0;

    for x, c in dataset[colName].iteritems():
        if pd.isnull(c):
            count = count + 1
    print(str(colName) + " : " + str(count))
    
for col in cols:
    check_NA(col)
# should return 0 
# if you run this before the for loop it should return the number of NA values in the column.


##### wd column imputation

## using pandas dataset that was created before, create groupby table by month and station and get wd mode
dataset_mode = dataset.groupby(['station', 'month'])['wd'].agg(pd.Series.mode)
dataset_mode

## check how many NA values in wd column before cleaning
check_NA('wd')

# define function that extracts the mode from dataset_mode 
def getmode(station, month):
    mode = dataset_mode.loc[(station, month)]
    return mode

# define clean_data function
def clean_wd():
    # x is index, and c is the value in that location, loop thro the column wd
    for x, c, in dataset['wd'].iteritems():
        # check if value is null
        if pd.isnull(c):
            #get station and month if the value is null
            station = dataset.loc[x, 'station']
            month = dataset.loc[x, 'month']
            # call get mode function
            mode = getmode(station, month)
            # replace NA with mode.
            dataset.loc[x, 'wd'] = mode
# run the function
clean_wd()
    
