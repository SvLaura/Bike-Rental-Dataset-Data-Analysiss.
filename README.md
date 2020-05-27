# Bike-Rental-Dataset-Data-Analysis.
The analysis investigates data from Bike rental website to find answers on questions bellow using Python and its libraries. Data are available data for public use: https://www.lyft.com/bikes/bay-wheels/system-data. 
Analyse is divided into 2 parts:
In the first part - an exploratory data analysis on a dataset is provided.
In the second part, the main findings from the  first part were taken to  create a slide deck that leverages polished, explanatory visualizations to communicate the results. 

#### The key features in the dataset 

Each trip is anonymized and includes:
    Trip Duration (seconds)
    Start Time and Date
    End Time and Date
    Start Station ID
    Start Station Name
    Start Station Latitude
    Start Station Longitude
    End Station ID
    End Station Name
    End Station Latitude
    End Station Longitude
    Bike ID
    User Type (Subscriber or Customer – “Subscriber” = Member or “Customer” = Casual)

#### During this analysis next questions were examined:

    1 Who is the Client that rent a bike? Find client characteristics:
        - what is the average age? what's the % and number of the clients?
        - youngest and olderest client? what's the % and number of the clients?
        - male or female? what's the % and number of the clients?
        
    
    2 What is the user preferences?
        - Does a client prefer choose bike share option?
        - What rental access method is the most popular?
        - Does a client prefer to take subscription ?
 
    3. How does world lockdown influense on bike rents?

    
#### Steps to investigate questions:

    1. Wrangle data.
    
        1.1. Data gathering.
        All files were downloaded from the website 'https://s3.amazonaws.com/baywheels-data/index.html' programmatically. Files are stored in zip files, and collected from 2017 till nowdays. Data for 2017 is a one zipped file, data for 2018 (and further years) are represented grouped by each month is a separate zippe file.
        
        1.2. Assessing and Cleaning data for quaity and tidiness.

    2. Exploratory data analysis. Visualisation.
   
    3. Explanatory analysis. A slide deck that leverages polished, explanatory visualizations to communicate the results.
