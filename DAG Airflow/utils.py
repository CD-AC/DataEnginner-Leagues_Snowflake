"""
*************************************************************
Author = @CD-AC                                             *
Date = '06/03/2024'                                         *
Description = Extracting Data from Multiple Football League *
*************************************************************
"""

# Import necessary libraries
import pandas as pd 
import time  
import random  
import os  
from datetime import datetime  

# Function to fetch and preprocess data from a given URL for a specific league
def get_data(url, liga):
    # Simulate a delay in fetching data to mimic real-world scenarios
    tiempo = [1, 3, 2]
    time.sleep(random.choice(tiempo))
    
    # Fetch tables from the URL and concatenate the first two into a single DataFrame
    df = pd.read_html(url)
    df = pd.concat([df[0], df[1]], ignore_index=True, axis=1)
    
    # Rename columns to more descriptive names
    df = df.rename(columns={0: 'EQUIPO', 1: 'J', 2: 'G', 3: 'E', 4: 'P', 5: 'GF', 6: 'GC', 7: 'DIF', 8: 'PTS'})
    
    # Adjust team names based on whether they start with numbers
    df['EQUIPO'] = df['EQUIPO'].apply(lambda x: x[5:] if x[:2].isnumeric() == True else x[4:])
    
    # Add a column indicating the league of the data
    df['LIGA'] = liga

    # Add a column with the current date to track when the data was fetched
    run_date = datetime.now().strftime("%Y-%m-%d")
    df['CREATED_AT'] = run_date

    return df

# Function to process data for multiple leagues based on a DataFrame containing URLs and league names
def data_processing(df):
    # Fetch and preprocess data for each league
    df_spain = get_data(df['URL'][0], df['LIGA'][0])
    df_premier = get_data(df['URL'][1], df['LIGA'][1])
    df_italy = get_data(df['URL'][2], df['LIGA'][2])
    df_germany = get_data(df['URL'][3], df['LIGA'][3])
    df_francia = get_data(df['URL'][4], df['LIGA'][4])
    df_portugal = get_data(df['URL'][5], df['LIGA'][5])
    df_holanda = get_data(df['URL'][6], df['LIGA'][6])

    # Combine all league data into a single DataFrame
    df_final = pd.concat([df_spain, df_premier, df_italy, df_francia, df_portugal, df_holanda], ignore_index=True)

    return df_final
