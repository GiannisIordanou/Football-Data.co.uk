# coding: utf-8

#Imports

import os
import re
import csv
import numpy as np
import requests
from bs4 import BeautifulSoup
import workerpool
import pandas as pd

#Functions

for i in i:
    
 
def get_country_urls(site):
    """
    Get country urls from site.
     
    Parameters
    ----------
    site: string
          Site url
           
    Returns
    -------
    countries: list
               1-D list of strings,
               name of countries
     
    countries_dict: dictionary
                    1-D dictionary containing
                    countries name and urls    
    """
    try:
        headers = {"User-agent": "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1"}
        r = requests.get(site, headers=headers)
        html = r.text       
        pattern = '<A HREF="(http://www.football-data.co.uk/.*?m.php)"><b>(.*?)</b>'
        matches = list(set(re.findall(pattern, html)))      
        countries_dict = {key:value for (value, key) in matches}
        countries = sorted(countries_dict.keys())
        return countries, countries_dict
    except Exception, e:
        print e
 
 
def get_season_from_csv_url(csv_url):
    """
    Extract season string from csv url
    and return apropriate season.
     
    Parameters
    ----------
    csv_url: url
             csv file url
              
    Returns
    -------
    season: int
            Season, ex. 2015
    """
    try:
        season_string = re.findall("/([0-9]{4})/", csv_url)[0]
        season =  int(season_string[-2:])
        if season > 90:
            cent = "19"
        else:
            cent = "20"
        season = cent + season_string[-2:]
        return season
    except Exception, e:
        print e
 

def get_country_csv_urls(country, countries_dict):
    """
    Get csv urls from a country's page.
     
    Parameters
    ----------
    country: string
             Name of country
              
    countries_dict: dictionary
                    1-D dictionary containing
                    countries name and urls
     
    Returns
    -------
    country_csv_urls: list
              The csv urls of country page
    """
    country_csv_urls = []
    try:
        country_url = countries_dict[country]
        r = requests.get(country_url)
        html = r.text
        soup = BeautifulSoup(html)
        matches = soup.findAll("a")
        for match in matches:
            if "csv" in match["href"]:
                country_csv_url = "".join([site, match["href"]])
                league = match.text
                csv_season = get_season_from_csv_url(match["href"])
                csv_details = [country_csv_url, country, league, csv_season]
                country_csv_urls.append(csv_details)
    except Exception, e:
        print e   
    return country_csv_urls
 
 
def folder_preparation(files_folder, countries, csv_urls):
    """
    Create the appropriate folders based on
    country names and leagues.
     
    Parameters
    ----------
    files_folder: string
                  Filepath of folder
                  to save files to
     
    countries: list
               List of countries names
     
    csv_urls: list
              1-D list
     
    Returns
    -------
    """
    if not os.path.exists(files_folder):
        try:
            os.mkdir(files_folder)
        except Exception, e:
            print e
             
    for country in countries:
        if country not in os.listdir(files_folder):     
            try:   
                os.mkdir('/'.join([files_folder, country]))
            except Exception, e:
                print e
     
    for country in countries:
        country_leagues = filter(lambda x: x[1] == country, csv_urls)
        for country_league in country_leagues:
            country, league = country_league[1:3]
            league_folder = '/'.join([files_folder, country, league])
            if not os.path.exists(league_folder):
                try:
                    os.mkdir(league_folder)
                except Exception, e:
                    print e
 

def download_csv_file(csv_info):
    """
    Download csv file.
     
    Parameters
    ----------
    csv_info: list
              1-D list of csv info
     
    Returns
    -------
    """
    csv_url, country, league, season = csv_info
    league_folders = os.listdir('/'.join([files_folder, country]))
    filename = unicode('/'.join([files_folder, country, league, season + '.csv']))
    try:
        r = requests.get(csv_url, stream=True)
        if r.status_code == 200:
            with open(filename, 'wb') as f:
                for chunk in r.iter_content(1024):
                    f.write(chunk)
    except Exception, e:
        print e
 

def download_multiple_csv_files(csv_urls, amount):
    """
    Download multiple csv files
     
    Parameters
    ----------
    csv_urls: list
              1-D list
     
    amount: int
            how many files to
            download at once
     
    Returns
    -------
    """
    pool = workerpool.WorkerPool(size=amount)
    pool.map(download_csv_file, csv_urls)
    pool.shutdown()
    pool.wait()

    
def correct_csv(csv_file):
    with open(csv_file, "rb") as f:
        csvfile = csv.reader(f)

        headers = csvfile.next()
        total_headers = len(headers)
        new_lines = [headers]
        for lines in csvfile:
            new_line = lines[:total_headers]
            new_lines.append(new_line)

    with open(csv_file, "wb") as f:
        csvfile = csv.writer(f)
        csvfile.writerows(new_lines)


def process_file(filepath):
    """
    Process file and add to dataframe.
    
    Parameters
    ----------
    filepath: string
              
    
    Returns
    ------- 
    df_file: dataframe
    """
    
    season = int(filepath.split('\\')[-1].split('.')[0])
    country = filepath.split('\\')[-3]
    league = filepath.split('\\')[-2]
    try:
        df_file = pd.read_csv(filepath, sep= ",", na_values=["", " ", "-"])
    except Exception, e:
        correct_csv(filepath) # Lines with more items than expected
        df_file = pd.read_csv(filepath, sep= ",", na_values=["", " ", "-"])
    
    df_file.dropna(axis=0,how='all', inplace=True) # Drop empty lines
    df_file.dropna(axis=1, how="all", inplace=True) # Drop all empty columns
    df_file.dropna(axis=1, thresh = 0.5 * df_file.shape[0], inplace=True) # Drop columns with a few items   
    df_file["Season"] = season
    df_file["Country"] = country
    df_file["League"] = league

    try:
        df_file.rename(columns={"HT": "HomeTeam", "AT": "AwayTeam"}, inplace=True) # Rename HT to HomeTeam and AT to AwayTeam
    except Exception, e:
        print e   
    
    return df_file


def get_ftsc(x):
    """
    Get Full Time Score.
    """
     
    fthg, ftag = x["FTHG"], x["FTAG"]
    if not np.isnan(fthg) and not np.isnan(ftag):
        ftsc = "-".join([str(fthg), str(ftag)]).replace(".0", "")
    else:
        ftsc = "-"
    return ftsc
 
 
def get_htsc(x):
    """
    Get Half Time Score.
    """
     
    hthg, htag = x["HTHG"], x["HTAG"]
    if not np.isnan(hthg) and not np.isnan(htag):
        #if hthg.dtype == float and htag.dtype == float:        
        htsc = "-".join([str(hthg), str(htag)]).replace(".0", "")
        #else:
            #htsc = "-"
    else:
        htsc = "-"
    return htsc
 
 
def get_date(x):
    """
    Reformat date.
    """
     
    date = x["Date"]
    if date:
        try:
            new_date = datetime.datetime.strftime(datetime.datetime.strptime(date, "%d/%m/%y"), "%d/%m/%Y")
        except:
            new_date = date
    else:
        new_date = "-"
    return new_date    
 
 
def total_goals_category(x):
    """
    Get total goals category.
    """
     
    total_goals = x["Total_Goals"]
    if not np.isnan(total_goals):
        if 0 <= total_goals < 2:
            total_goals_category = "0-1"
        elif 2 <= total_goals < 4:
            total_goals_category = "2-3"
        elif 4 <= total_goals < 7:
            total_goals_category = "4-6"
        elif 7 <= total_goals:
            total_goals_category = "7"
        else:
            total_goals_category = "-"
    else:
        total_goals_category = "-"
 
    return total_goals_category        
        
 
#Script
site = "http://www.football-data.co.uk/"
files_folder = "Files"
  
countries, countries_dict = get_country_urls(site)
csv_urls = []
for country in countries:
    country_csv_urls = get_country_csv_urls(country, countries_dict)
    csv_urls.extend(country_csv_urls)    
     
folder_preparation(files_folder, countries, csv_urls)
download_multiple_csv_files(csv_urls, 10)

football_files = []
for (dirpath, dirnames, filenames) in os.walk(files_folder):
    football_files.extend(map(lambda x: os.path.join(dirpath, x), filenames))

df = pd.DataFrame()
for each_file in football_files:
    try:
        df_file = process_file(each_file)
        df = df.append(df_file, ignore_index=True)
    except Exception, e:
        print e
 
# Add custom columns
df["HTSC"] = df.apply(get_htsc, axis=1) # Fix HTSC
df["FTSC"] = df.apply(get_ftsc, axis=1) # Fix FTSC
df["Date"] = df.apply(get_date, axis=1) # Fix Date
 
df["B365_HA_Odds_Class"] = (df.B365H / df.B365A).apply(lambda x: round(x, 2))
df["B365_HD_Odds_Class"] = (df.B365H / df.B365D).apply(lambda x: round(x, 2))    
df["B365_AD_Odds_Class"] = (df.B365A / df.B365D).apply(lambda x: round(x, 2))   
df["Total_Goals"] = df.FTHG + df.FTAG
df["Under_Over"] = df["Total_Goals"].apply(lambda x: "O" if x > 2 else "U")
df["Total_Corners"] = df.HC + df.AC
 
df["Total_Goals_Category"] = df.apply(total_goals_category, axis=1)
df["Goal_No_Goal"] = df.apply(lambda x: "G" if x["FTHG"] != 0 and x["FTAG"] != 0 else "NG", axis=1)
 
# Rearrange columns
proper_columns_order = ['Season', 'Country', 'League', 'Div', 'Date',
                        'HomeTeam', 'AwayTeam', 'FTSC', 'FTR', 'FTHG',
                        'FTAG', 'HTSC', 'HTR', 'HTHG', 'HTAG', 'Under_Over', 
                        'Goal_No_Goal', 'Total_Goals', 'Total_Goals_Category', 'Total_Corners',
                        'B365_HA_Odds_Class', 'B365_HD_Odds_Class', 'B365_AD_Odds_Class', 
                        'Attendance', 'Referee', 'HS', 'AS', 'HST', 'AST', 'HHW', 'AHW', 
                        'HC', 'AC', 'HF', 'AF', 'HO', 'AO',  'HY', 'AY', 'HR', 'AR', 'HBP', 
                        'ABP', 'B365H', 'B365D', 'B365A']
 
other_columns = [i for i in df.columns if i not in proper_columns_order]
df = df[proper_columns_order + other_columns]
df.fillna("-", inplace=True)
 
# Save to file
df.to_csv(os.path.join(files_folder, "Football-Data.csv"), index=False)
df.to_excel(os.path.join(files_folder, "Football-Data.xlsx"), index=False)
