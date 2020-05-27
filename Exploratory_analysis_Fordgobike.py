#!/usr/bin/env python
# coding: utf-8

# ## Bike Rental Dataset Data Analysis.
#     Published: on May 27, 2020
#     
# ###  Summary
# 
#     The analysis investigates data from Bike rental website to find answers on questions bellow using Python and its libraries. Data are available data for public use: https://www.lyft.com/bikes/bay-wheels/system-data. 
# Analyse is divided into 2 parts:
# In the first part - an exploratory data analysis on a dataset is provided.
# In the second part, the main findings from the  first part were taken to  create a slide deck that leverages polished, explanatory visualizations to communicate the results. 
# 
# #### The key features in the dataset 
# 
# Each trip is anonymized and includes:
#     Trip Duration (seconds)
#     Start Time and Date
#     End Time and Date
#     Start Station ID
#     Start Station Name
#     Start Station Latitude
#     Start Station Longitude
#     End Station ID
#     End Station Name
#     End Station Latitude
#     End Station Longitude
#     Bike ID
#     User Type (Subscriber or Customer – “Subscriber” = Member or “Customer” = Casual)
# 
# #### During this analysis next questions were examined:
# 
#     1 Who is the Client that rent a bike? Find client characteristics:
#         - what is the average age? what's the % and number of the clients?
#         - youngest and olderest client? what's the % and number of the clients?
#         - male or female? what's the % and number of the clients?
#         
#     
#     2 What is the user preferences?
#         - Does a client prefer choose bike share option?
#         - What rental access method is the most popular?
#         - Does a client prefer to take subscription ?
#  
#     3. How does world lockdown influense on bike rents?
# 
#     
# #### Steps to investigate questions:
# 
#     1. Wrangle data.
#     
#         1.1. Data gathering.
#         All files were downloaded from the website 'https://s3.amazonaws.com/baywheels-data/index.html' programmatically. Files are stored in zip files, and collected from 2017 till nowdays. Data for 2017 is a one zipped file, data for 2018 (and further years) are represented grouped by each month is a separate zippe file.
#         
#         1.2. Assessing and Cleaning data for quaity and tidiness.
# 
#     2. Exploratory data analysis. Visualisation.
#    
#     3. Explanatory analysis. A slide deck that leverages polished, explanatory visualizations to communicate the results.

# In[ ]:


# for map visualisation:
import pip
package_name='gmplot' 
pip.main(['install',package_name])

import pip
package_name='geopy' 
pip.main(['install',package_name])


# In[3]:


# import:
import numpy as np
import os
import pandas as pd
import requests
import tweepy
import json
from timeit import default_timer as timer
import matplotlib.pyplot as plt
import seaborn as sb
import re
import datetime
import zipfile
import io
from glob import iglob
import sys
import seaborn as sb
import gmplot 
from geopy.geocoders import Nominatim

# display plots inline
get_ipython().run_line_magic('matplotlib', 'inline')


# create folder for data files ( if it is not exist ) :
folder_name = 'data'
if not os.path.exists(folder_name):
    os.makedirs(folder_name)


# ### 1. Wrangle data.
# 
# #### 1.1. Data gathering.

# In[4]:


# main url with files to download:
url = 'https://s3.amazonaws.com/baywheels-data/index.html'

# check url:
page = requests.get(url)
print(page.status_code)


# In[ ]:


# Output: 200 - url is ok


# In[ ]:


# Download data from url:

# function to create folder for each year ( if it is not exist )
def folder_name_func(year):
    folder_name_y = folder_name + '/' + year
    if not os.path.exists(folder_name_y):
        os.makedirs(folder_name_y)
    return folder_name_y
        
# Function:
def get_files_func(data_name, file_url_num, file_url = 'def'):
    # if parameter 'file_url' of function is skipped then 'file_url =': 
    if file_url == 'def':
        file_url = 'https://s3.amazonaws.com/'+ data_name +'-data/'+ file_url_num + '-' + data_name + '-tripdata.csv.zip'
    # check if url exists:  
    r = requests.get(file_url, stream=True)
    url_exists = r.status_code # = 200 - url exists
    if url_exists == 200: 
        check = zipfile.is_zipfile(io.BytesIO(r.content))
        if (check):
            z = zipfile.ZipFile(io.BytesIO(r.content))
            z.extractall(folder_name_y)
        # if not zip format try scv format
        else:
            try:
                download = requests.get(ur,stream=True)
                # take file name from the url:
                tmp = file_url.split('/'[-1])
                f_name = tmp[len(tmp) - 1]
                # download file:
                with open(folder_name_y +'/'+ f_name, 'w') as file:
                    file.write(download.text)
            except:
                print(file_url + "file wasn't downloaded")
    return url_exists

# start from 201801, then 201802,...until today minus one or few months
now = datetime.datetime.now()
start_year = 2018
files_in_year = np.arange(1,13) 
year_count = now.year - start_year + 1

# get file for not standart url for 2017 year:
urls = ['https://s3.amazonaws.com/baywheels-data/2017-fordgobike-tripdata.csv.zip']
folders=[]
folder_name_y = folder_name_func('2017')
folders.append(folder_name_y)
get_files_func('fordgobike','2017',urls[0])

# get files for other years:
for i in range(year_count):
    year = str(int(start_year) + i)

    for fy in files_in_year:
        fy = str(fy)       
        if len(fy) == 1:
            file_url_num = year + "0" + fy
        else:
            file_url_num = year + fy
        folder_name_y = folder_name_func(year)
        if folder_name_y not in folders:
            folders.append(folder_name_y)
        func_resp = get_files_func('fordgobike', file_url_num)
        if func_resp != 200:
            get_files_func('baywheels', file_url_num)
    


# In[10]:


# read all *.scv files from the 'folders' to dataframe 'df':
dirpath = os.getcwd()
df = pd.DataFrame()
for i in range(len(folders)):
    path = os.getcwd() + '/' + folders[i] + '/*.csv'
    # to dataframe:
    df2 = pd.concat((pd.read_csv(f, dtype='unicode', index_col=False, low_memory=False) 
                     for f in iglob(path, recursive=True)), ignore_index=True, sort=False)   
    df = df.append(df2)


# #### 1.2. Assessing and Cleaning data for quaity and tidiness.

# In[11]:


df.head(5)


# In[12]:


# check data:
df.info()


# - Output: 5 879 670 rows, 26 columns.
#     * Data types - change.

# In[13]:


df[df.duplicated()]


#    * Duplicates ( 7512 rows )- remove.

# In[14]:


df.query('start_time == "2020-03-01 15:59:07"')


#   * Check if there is a row with end_station_name is null/NaN and its end_station_latitude & end_station_longitude are equal to end_station_latitude & end_station_longitude of another row with end_station_name that is not Null/NaN  - then end_station_name of first row make equal to the second one.

# In[15]:


# check empty rows in as a % of all data:
100 * df.isnull().sum() / len(df)


# - Output: member_birth_year, member_gender, bike_share_for_all_trip, rental_access_method, rideable_type, member_casual - % of missing value is extremely high for data analyses.
#     * Check these columns for each year separately.

# In[16]:


# copy df:
df2 = df.copy()


# In[17]:


# remove duplicates:
df2.drop_duplicates(inplace = True)


# In[50]:


# change data types:

# object to datetime and create new columns:

df2['start_time']  = pd.to_datetime(df2['start_time'])

df2['start_year']  = df2['start_time'].dt.year
df2['start_month']  = df2['start_time'].dt.month
df2['start_day']  = df2['start_time'].dt.day
df2['start_weekday'] = df2['start_time'].dt.weekday
df2['start_hour'] = df2['start_time'].dt.hour

df2['end_time']  = pd.to_datetime(df2['end_time'])


# In[19]:


# object to float:     
df2['start_station_latitude']  = pd.to_numeric(df2['start_station_latitude'], errors='coerce', downcast='float')
df2['start_station_longitude']  = pd.to_numeric(df2['start_station_longitude'], errors='coerce', downcast='float')

df2['end_station_latitude']  = pd.to_numeric(df2['end_station_latitude'], errors='coerce', downcast='float')
df2['end_station_longitude']  = pd.to_numeric(df2['end_station_longitude'], errors='coerce', downcast='float')


# In[20]:


# replace Nan -> '' for string data
df2.fillna({'member_gender':'','bike_share_for_all_trip':'','rental_access_method':''}, inplace=True)


# In[21]:


# replace Nan -> 0 for numeric data
df2.fillna({'start_station_id':0,'end_station_id':0, 'member_birth_year':0}, inplace=True)


# In[37]:


# object to int:
df2['bike_id']  = pd.to_numeric(df2['bike_id'], errors='coerce', downcast='integer')
df2['start_station_id']  = pd.to_numeric(df2['start_station_id'], errors='coerce', downcast='integer')
df2['end_station_id']  = pd.to_numeric(df2['end_station_id'], errors='coerce', downcast='integer')
df2['member_birth_year']  = pd.to_numeric(df2['member_birth_year'], errors='coerce', downcast='integer')
df2['duration_sec']  = pd.to_numeric(df2['duration_sec'], errors='coerce', downcast='integer')

df2['duration_min'] = round(df2['duration_sec']/60)
df2['duration_min']  = pd.to_numeric(df2['duration_min'], errors='coerce', downcast='integer')


# In[32]:


# drop columns:
df2.drop(['duration_sec','start_time','started_at','ended_at', 'start_lat','start_lng','end_lat','end_lng','rideable_type','ride_id','member_casual'], axis = 1, inplace = True)


# In[48]:


df2.dtypes


# In[49]:


df2.sample(5)


# In[51]:


# check columns that have high % of missing data separetaly by years.
# member_birth_year, member_gender, rental_access_method

# 1.member_birth_year:
100 * df2.query('member_birth_year == 0').start_year.value_counts()/df2.start_year.value_counts()


# - Output: for member age analyse we can take only 2018 year with 5.9% of missing value of member_birth_year data.

# In[52]:


# create dataframe for member age analyse:
df_memb_age = df2.query('start_year == 2018 & member_birth_year != 0')
df_memb_age['member_age']  = int(datetime.datetime.now().year) - df_memb_age['member_birth_year']
df_memb_age.member_age.value_counts()


# In[53]:


print("Data actual for 2018 year:")
print("The average age of member that rent a bike is ", df_memb_age.member_age.mean())
print("Oldest members ",df_memb_age.member_age.max()," and ",df_memb_age.query('member_age!= member_age.max()').member_age.max(), "years old.")
print("Youngest members ",df_memb_age.member_age.min()," and ",df_memb_age.query('member_age!= member_age.min()').member_age.min(), "years old.")


# In[54]:


df_memb_age.query('member_age in (139, 132, 20, 21)').member_age.value_counts()


# - Output: 
#     youngest members at age 20 made 13 348 rides in 2018;
#     youngest members at age 21 made 5 390 rides in 2018;
#     oldest members at age 132 made 23 rides in 2018;
#     oldest members at age 139 made 5 rides in 2018.

# In[55]:


# 2.member_gender:
100 * df2.query('member_gender == ""').start_year.value_counts()/df2.start_year.value_counts()


# - Output: results similar to previous where member age was explored. 
#   For member gender analysis we can take only 2018 year with 5.9% of missing value of member gender data.

# In[56]:


# create dataframe for member gender analyse:
df_memb_gender = df2.query('start_year == 2018 & member_gender != ""')

# check popuar gender:
100 * df_memb_gender.member_gender.value_counts()/df_memb_gender.shape[0]


# - Output: Male more often rented a bike in 2018.

# In[57]:


# 3.rental_access_method:
100 * df2.query('rental_access_method == ""').start_year.value_counts()/df2.start_year.value_counts()


# - Output: for rental_access_method analyse we can't take any data - min % of missing data is 32%.

# In[58]:


# 3.bike_share_for_all_trip:
100 * df2.query('bike_share_for_all_trip == ""').start_year.value_counts()/df2.start_year.value_counts()


# - Output: for bike_share_for_all_trip analyse we can take only 2019 year with 9.7 % of missing value of bike_share_for_all_trip data.

# In[59]:


# create dataframe for bike_share_for_all_trip analyse:
df_bike_share = df2.query('start_year == 2019 & bike_share_for_all_trip != ""')

# check %:
100 * df_bike_share.bike_share_for_all_trip.value_counts()/df_bike_share.shape[0]


# - Output: only 8% of people used options 'Bike Share For All Trip' in 2019.

# In[60]:


print('Number of rides by user types:',"\n", df2.user_type.value_counts() ,"\n")

print('Percentage of rides by user types:',"\n", 100 * df2.user_type.value_counts()/df2.shape[0])


# In[61]:


# Add a new column 'user_type_int' with 1 and 0 values (1 - user_type = Subscriber, 0 - user_type = Customer):
df2['user_type_int'] = 1
df2.loc[df2['user_type'] == "Customer", 'user_type_int'] = 0


# ### 2. Exploratory data analysis. Visualisation.

# In[62]:


# Member age in 2018 :
plt.figure(figsize=(25,5))
base_color = sb.color_palette()[0]
sb.countplot(data = df_memb_age, x = 'member_age', color = base_color)
plt.title('Member age distribution by the number of bike rides in 2018 year', fontsize=18)
plt.ylabel('Count of bike rides', fontsize=18)
plt.xlabel('Member age', fontsize=18)


#  - Output: number of bike rides were made by people at 27 - 35 years old.

# In[63]:


# Member gender in 2018 :
source_labels = df_memb_gender.member_gender.value_counts().index
source_counts = df_memb_gender.member_gender.value_counts()
colors = ['royalblue','pink',  'lightgreen']

plt.pie(source_counts, labels = source_labels, autopct='%1.1f%%', shadow=True, colors=colors)

plt.title('Gender distribution by the number of bike rides in 2018', fontsize=18)
plt.axis('equal')
plt.show();


# In[64]:


# bike_share_for_all_trip in 2019 :
source_labels = df_bike_share.bike_share_for_all_trip.value_counts().index
source_counts = df_bike_share.bike_share_for_all_trip.value_counts()
colors = ['grey','gold']

plt.pie(source_counts, labels = source_labels, autopct='%1.1f%%', shadow=True, colors=colors)

plt.title('Bike share option distribution by the number of bike rides in 2019', fontsize=18)
plt.axis('equal')
plt.show();


# In[65]:


# user_type  :
source_labels = df2.user_type.value_counts().index
source_counts = df2.user_type.value_counts()
colors = ["#9b59b6", "#3498db"]

plt.pie(source_counts, labels = source_labels, autopct='%1.1f%%', shadow=True, colors=colors)

plt.title('User type option distribution by the number of bike rides - from 2017 till 202003', fontsize=18)
plt.axis('equal')
plt.show();


# In[66]:


# User type by years :
plt.figure(figsize=(25,5))
base_color = sb.color_palette()[0]

ct_counts = df2.groupby(['start_year','user_type']).size()
ct_counts = ct_counts.reset_index(name='count')

ct_counts.pivot(index='start_year',columns='user_type',values='count')

sb.countplot(data = df2, x = 'start_year', hue='user_type', color = base_color)

plt.title('Bike rides by user type in each year (in 2020 only 3 months)', fontsize=18)
plt.ylabel('Count of bike rides', fontsize=18)
plt.xlabel('Years', fontsize=18)


# In[ ]:


- Output: for  2020 - data were taken only for 3 months.
    People prefer rent a bike through subscription.


# In[592]:


# 2020 - detaily March, April - wrold lockdown :
plt.figure(figsize=(25,5))
base_color = sb.color_palette()[0]
sb.countplot(data = df2.query("start_year == 2020"), x = 'start_month', color = base_color)

plt.title('Bike rides by month in 2020', fontsize=18)
plt.ylabel('Count of bike rides', fontsize=18)
plt.xlabel('Month', fontsize=18)


# In[67]:


# 2019,2018 - by month :
plt.figure(figsize=(25,5))
base_color = sb.color_palette()[0]
sb.countplot(data = df2.query("start_year in (2018, 2019)"), x = 'start_month', color = base_color)

plt.title('Bike rides by month in 2018, 2019', fontsize=18)
plt.ylabel('Count of bike rides', fontsize=18)
plt.xlabel('Month', fontsize=18)


# - Output: The number of bike rents decreased rapidly  in March of 2020 more than in two times  compared with February data, it happened due to world lockdown (covid-19). On the other hand, we see that there is a tendency of increasing number of bike rents in March in previous two year.
# - There is also a tendency of decreasing number of bike rentals in winter months. The peak of bike rentals comes to June.

# In[68]:


# 2019,2018 - by weekdays :
plt.figure(figsize=(25,5))
base_color = sb.color_palette()[0]
sb.countplot(data = df2.query("start_year in (2018, 2019)"), x = 'start_weekday', color = base_color)

plt.title('Bike rides by weekdays in 2018, 2019', fontsize=18)
plt.ylabel('Count of bike rides', fontsize=18)
plt.xlabel('Weekdays', fontsize=18)


# - Output: Number of bike rides is lower almost in two times on weekends.

# In[69]:


# 2019,2018 - by hours :
plt.figure(figsize=(25,5))
base_color = sb.color_palette()[0]
sb.countplot(data = df2.query("start_year in (2018, 2019)"), x = 'start_hour', color = base_color)

plt.title('Bike rides by hours in 2018, 2019', fontsize=18)
plt.ylabel('Count of bike rides', fontsize=18)
plt.xlabel('Hours', fontsize=18)


# - Output: The peak of bike rentals comes between 7 a.m. and 9 a.m. and between 16 p.m. and 18 p.m. 

# In[71]:


# function to determin city:
geolocator = Nominatim(user_agent="geoapiExercises")

def city_state_country(coord):
    location = geolocator.reverse(coord, exactly_one=True)
    address = location.raw['address']
    city = address.get('city', '')
    #state = address.get('state', '')
    #country = address.get('country', '')
    return city #, state, country


# In[76]:


df2_2020 = df2.query('start_year == 2020 & start_station_latitude != 0.0 & start_station_longitude != 0.0')


# In[ ]:


# create a new column - city:

df2_2020['city'] = ""

for i in range(len(df2_2020) - 1):
    coord = str(df2_2020['start_station_latitude'].iloc[i]) +","+str(df2_2020['start_station_longitude'].iloc[i])
    try:
        df2_2020['city'].iloc[i] = city_state_country(coord)
    except:
        print(coord)


# In[766]:


#city_state_country(df2['coord'].iloc[1])[0]
df2.head(5)


# In[673]:


# user_type vs member_age:
sb.boxplot(data = df_memb_age, x = 'user_type', y = 'member_age', color = base_color)
plt.title('User type vs age in 2018', fontsize=18)
plt.ylabel('Age', fontsize=15)
plt.xlabel('User type', fontsize=15)


# In[674]:


# user_type vs member_gender:
sb.boxplot(data = df_memb_age, x = 'member_gender', y = 'member_age', color = base_color)
plt.title('User gender vs age in 2018', fontsize=18)
plt.ylabel('Age', fontsize=18)
plt.xlabel('User gender', fontsize=18)


# In[697]:


plt.scatter(data = df_memb_age, x = 'member_age', y = 'duration_min', color = base_color)
plt.title('Member age vs duration (min) in 2018', fontsize=18)


# ### Conclusion
# 
# Dataset  -  5 879 670 rows, 26 columns.
# 
# 1. Client characteristics:
# 
#         The average age of member that rent a bike is 37 years old.
#         Oldest members  139  and  132 years old.
#         Youngest members  20  and  21 years old.
# 
#         Youngest members at age 20 made 13 348 rides in 2018; 
#         Youngest members at age 21 made 5 390 rides in 2018; 
#         Oldest members at age 132 made 23 rides in 2018; 
#         Oldest members at age 139 made 5 rides in 2018.
# 
#         The most number of bike rides were made by people at 27 - 35 years old.
# 
#         Male (73%) more often rented a bike in 2018.
# 
# 2. User preferences: 
# 
#         Only 8% of people used options 'Bike Share For All Trip'.
#         People prefer rent a bike through subscription.
# 
#         Number of rides by user types: 
#           Subscriber - 4 552 602 (78%)
#           Customer   - 1 235 297 (22%)     
# 
#         There is also a tendency of decreasing number of bike rentals in winter months and increasing in spring time. The peak of bike rentals comes to June.
#         Number of bike rides is lower almost in two times on weekends.
#         The peak of bike rentals comes between 7 a.m. and 9 a.m. and between 16 p.m. and 18 p.m. 
# 
# 3. The number of bike rents decreased rapidly in March of 2020 more than in two times compared with February data, it happened due to world lockdown (covid-19). 
# 
# 
# Data limitation: 
#     Data were taken for 2017, 2018, 2019, 2020 years. For 2017 and 2020 data are not full.
#     Data analysis on clients age and gender was made only for 2018 year.
#     Data analysis on bike share option was made only for 2019 year.
