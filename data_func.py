
# import:
import numpy as np
import os
import pandas as pd
import requests
from glob import iglob
import zipfile
import io
import re
import datetime

url = 'https://s3.amazonaws.com/baywheels-data/index.html'
folder_name = 'data'


def data_to_df(source_type = 'download'):
    '''if source_type='download' - download files to local folder 'data' 
    read all *.scv files from the 'folders' to dataframe 'df'
    or read all data from local files
    '''
    df = pd.DataFrame()
    page = requests.get(url)
    if source_type == 'download':
        folders = download_data()
    else:
        folders = ['data/2017', 'data/2018', 'data/2019', 'data/2020', 'data/2021', 'data/2022']
    print('------>end download data')
    if page.status_code == 200:
        dirpath = os.getcwd()
        for i in range(len(folders)):
            path = os.getcwd() + '/' + folders[i] + '/*.csv'
            # to dataframe:
            df2 = pd.concat((pd.read_csv(f, dtype='unicode', index_col=False, low_memory=False)
                             for f in iglob(path, recursive=True)), ignore_index=True, sort=False)
            df = pd.concat([df, df2])
    else:
        print('error: status_code <> 200!!')
    return df


# function to create folder for each year ( if it is not exist )
def folder_name_func(year):
    folder_name_y = folder_name + '/' + year
    if not os.path.exists(folder_name_y):
        os.makedirs(folder_name_y)
    return folder_name_y


def get_files_func(folder_name_y, data_name, file_url_num, file_url = 'def'):
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
                download = requests.get(url,stream=True)
                # take file name from the url:
                tmp = file_url.split('/'[-1])
                f_name = tmp[len(tmp) - 1]
                # download file:
                with open(folder_name_y +'/'+ f_name, 'w') as file:
                    file.write(download.text)
            except:
                print(file_url + "file wasn't downloaded")
    return url_exists


def download_data():
    '''
    download data to local folders
    '''
    # start from 201801, then 201802,...until today minus one or two months
    now = datetime.datetime.now()
    start_year = 2018
    files_in_year = np.arange(1,13)
    year_count = now.year - start_year + 1

    # get file for not standart url for 2017 year:
    urls = ['https://s3.amazonaws.com/baywheels-data/2017-fordgobike-tripdata.csv.zip']
    folders=[]

    # special for 2017 - download data:
    folder_name_y = folder_name_func('2017')
    folders.append(folder_name_y)
    get_files_func(folder_name_y, 'fordgobike','2017',urls[0])

    # get files for other years:
    for i in range(year_count - 1):
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
            func_resp = get_files_func(folder_name_y, 'fordgobike', file_url_num)
            if func_resp != 200:
                get_files_func(folder_name_y, 'baywheels', file_url_num)

    return folders
