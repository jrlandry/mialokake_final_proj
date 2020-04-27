import unittest
import sqlite3
import json
import os
import requests
import csv
import matplotlib.pyplot as plt
import numpy as np

#CREATE DATABASE

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

#API 1 - COVID

#READ Covid data into json dict
def request_covid_data():
    try:
        url = "https://api.covid19api.com/summary" 
        r = requests.get(url)
        json_data = json.loads(r.text)
        #return json_data
        
    except:
        print("Error")
        json_data = {}
    
    return json_data
    
#COVID Table 

def setUpCovidDataTable(cur, conn):
    #make headers
    cur.execute('CREATE TABLE IF NOT EXISTS CovidData(Country TEXT, Country_Code TEXT, New_Confirmed INTEGER, Total_Confirmed INTEGER, New_Deaths INTEGER, Total_Deaths INTEGER, New_Recovered INTEGER, Total_Recovered INTEGER, Date TEXT)')
    #where to get values
def insert_covid_rows(data, cur, conn):   
    cur.execute("SELECT COUNT(*) FROM CovidData")
    i = cur.fetchone()[0] 
    for country in data['Countries'][i:i+20]:
       
        nation = country["Country"]
        country_code = country["CountryCode"]
        new_confirmed = country["NewConfirmed"]
        total_confirmed = country["TotalConfirmed"]
        new_deaths = country["NewDeaths"]
        total_deaths = country["TotalDeaths"]
        new_recovered = country['NewRecovered']
        total_recovered = country['TotalRecovered']
        date = country['Date']     
            #fill in rows
        cur.execute("INSERT INTO CovidData(Country, Country_Code, New_Confirmed, Total_Confirmed, New_Deaths, Total_Deaths, New_Recovered, Total_Recovered, Date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (nation, country_code, new_confirmed, total_confirmed, new_deaths, total_deaths, new_recovered, total_recovered, date))
    conn.commit()


#API 2 - OPEN AQ

#get air data and make json dict
def request_openAQ_data1():
    try:
        url = "https://api.openaq.org/v1/countries?order_by=count&sort=desc"
        r = requests.get(url)
        json_data = json.loads(r.text)

    except:
        print("Error")
        json_data = {}
    
    return json_data

def request_openAQ_data2():
    try:
        url = "https://api.openaq.org/v1/measurements?parameter=co&order_by=parameter&sort=desc"
        r = requests.get(url)
        json_data = json.loads(r.text)

    except:
        print("Error")
        json_data = {}
    
    return json_data


#Open AQ Table #1 (Listings of Supported Countries in the Platform)
def setUpCountryOpenAQTable(cur, conn):
    #make headers
    cur.execute('CREATE TABLE IF NOT EXISTS AirDataCountries(Country TEXT, Country_Code TEXT, Measurements_Count INTEGER, Cities INTEGER, Locations INTEGER)')

# where to get values
def insert_AQCountries_rows(data, cur, conn):   
    cur.execute("SELECT COUNT(*) FROM AirDataCountries")
    i = cur.fetchone()[0]
    for country in data['results'][i:i+20]:
        try:
            nation = country["name"]
        except:
            nation = "-"
        country_code = country["code"]
        count = country["count"]
        cities = country["cities"]
        locations = country["locations"]
        #fill in rows
        cur.execute("INSERT INTO AirDataCountries (Country, Country_Code, Measurements_Count, Cities, Locations) VALUES (?, ?, ?, ?, ?)", (nation, country_code, count, cities, locations))
    conn.commit()

#2nd open AQ table- parameter

def setUpAQParameterTable(cur, conn):
    #make headers
    cur.execute('CREATE TABLE IF NOT EXISTS AQParameter(Name TEXT, Country_Code TEXT, City TEXT, Value REAL, Unit TEXT, Date TEXT, Parameter TEXT)')

#where to get values
def insert_AQParameter_rows(data, cur, conn):   
    cur.execute("SELECT COUNT(*) FROM AQParameter")
    i = cur.fetchone()[0]
    for measurement in data['results'][i:i+20]:
        country_code = measurement["country"]
        city = measurement["city"]
        val = measurement["value"]
        unit = measurement["unit"]
        date = measurement["date"]["utc"]
        param = measurement["parameter"]
        cur.execute("SELECT Country FROM AirDataCountries WHERE Country_Code = ?", (country_code, ))
        country_name = str(cur.fetchone())
        #fill in rows
        cur.execute("INSERT INTO AQParameter(Name, Country_Code, City, Value, Unit, Date, Parameter) VALUES (?, ?, ?, ?, ?, ?, ?)", (country_name, country_code, city, val, unit, date, param))
    conn.commit()


#API 3 - World Bank

#get requests into json dictionary 
def request_url_countries():
    try:
       url = "http://api.worldbank.org/v2/countries?format=json&per_page=100"
       r = requests.get(url)
       info = json.loads(r.text)
    except:
       print("Error")
       info = {}
    #print(info)
    return info
    
#World Bank Countries Table
def setUpCountryTable(cur, conn):
    #make headers
    cur.execute('CREATE TABLE IF NOT EXISTS CountryData(Id TEXT, Name TEXT, Region TEXT, Income_Level TEXT)')
    #where to get values

def insert_Country_rows(info, cur, conn):   
    #x = 0
    cur.execute("SELECT COUNT(*) FROM CountryData")
    i = cur.fetchone()[0]
    for place in info[1][i:i+20]:
       
        Id = place["id"]
        Name = place["name"]
        Region = place['region']['value']
        Income_Level = place["incomeLevel"]['value']
        
        
         #fill in rows
        cur.execute("INSERT INTO CountryData(Id, Name, Region, Income_Level) VALUES (?, ?, ?, ?)", (Id, Name, Region, Income_Level))
        #x += 1
    conn.commit()


#WRITING CSV/ CALCULATIONS

#covid data - count of total recovered in high income countries

def get_income_countries(income_level, cur, conn):
    cur.execute('SELECT CovidData.Country, CovidData.Total_Recovered FROM CovidData JOIN CountryData ON CountryData.Name = CovidData.Country WHERE CountryData.Income_Level = ?', (income_level, ))
    result = cur.fetchall()
    return result

def get_total_recovered(countries, cur, conn):
    total = 0
    for i in countries:
        total += i[-1]
    return total

def get_parameter_values(parameter, country_code, cur, conn):
    cur.execute("SELECT AQParameter.Value, AQParameter.Country_Code FROM AQParameter JOIN AirDataCountries ON AQParameter.Country_Code = AirDataCountries.Country_Code WHERE AQParameter.Parameter = ? and AirDataCountries.Country_Code = ?", (parameter, country_code))
    result = cur.fetchall()
    return result


def get_average_param_values(countries, cur, conn):
    #countries is list of tuples with param val country code
    total = 0
    country_count = 0
    for i in countries:
        total += i[0]
        country_count += 1

    return str(total/country_count)

def get_top_countries(cur, conn):
    cur.execute('SELECT CovidData.Country, CovidData.Total_Recovered, CovidData.Total_Confirmed FROM CovidData')
    result = cur.fetchall()
    return sorted(result, key = lambda x: x[-1], reverse = True)

def get_top_counts(data, cur, conn):
    counts = []
    x = 0
    while x < 10:
        counts.append(data[x])
        x += 1
    return counts


#write calculations file
def write_csv_calculation_data(income_data1, income_data2, income_data3, income_data4, cur, conn):
    total_rec1 = get_total_recovered(income_data1, cur, conn)
    total_rec2 = get_total_recovered(income_data2, cur, conn)
    total_rec3 = get_total_recovered(income_data3, cur, conn)
    total_rec4 = get_total_recovered(income_data4, cur, conn)
    countries_and_param = get_parameter_values("co", "CN", cur, conn)
    data = get_average_param_values(countries_and_param, cur, conn)
    x = get_top_counts(get_top_countries(cur, conn), cur, conn)
    with open('final_proj_calculations.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Calculation", "Value"])
        writer.writerow(["Total Recovered in High Income Countries", total_rec1])
        writer.writerow(["Total Recovered in Upper middle income Countries", total_rec2])
        writer.writerow(["Total Recovered in Lower middle income Countries", total_rec3])
        writer.writerow(["Total Recovered in Low income Countries", total_rec4])
        num = 1
        for value in countries_and_param:
            writer.writerow(["City in CN reading " + str(num), value[0]])
            num+=1
        writer.writerow(['Average co level in CN', data])

        for i in x:
            writer.writerow(["Recovery Rate: " + i[0], i[1]/i[-1]])
   
   
#MATPLOTLIB 

#Convert calculations csv file into a json file, to then be able to read calculations in a dictionary format 
def csv_to_json_converter(csvfile, jsonfile):
    csvFilepath = csvfile
    jsonFilepath = jsonfile
    data = {}
    with open(csvFilepath) as csvFile:
        csvReader = csv.DictReader(csvFile)
        for rows in csvReader:
            calculation = rows['Calculation']
            data[calculation] = rows['Value']
    with open(jsonFilepath, 'w') as jsonFile:
        jsonFile.write(json.dumps(data, indent =4))

    try:
        root_path = os.path.dirname(os.path.abspath(__file__))
        #dir = os.path.dirname(__file__)
        full_path = os.path.join(root_path, jsonfile)
        in_file = open(full_path, 'r')
        data = in_file.read()
        calculationsDict = json.loads(data)
        in_file.close()
    except:
        print("Problem reading the input file")
        calculationsDict = {}

    return calculationsDict

def visualization1(json_dict):
    #BAR GRAPH #1
    calculations_keys_list = list(json_dict.keys())
    calculations_values_list = list(json_dict.values())

    recovery_rate = []
    recovery_rate_keys = []

    for i in calculations_values_list[-10:]:
        recovery_rate.append(float(i))
    for i in calculations_keys_list[-10:]:
        country = i.split(": ")
        recovery_rate_keys.append(country[1])

    labels = recovery_rate_keys
    vals = recovery_rate

    x = np.arange(len(labels))
    width = 0.4

    fig, ax = plt.subplots()
    rects = ax.bar(labels, vals, width, color = 'orange')

    ax.set_ylabel('Recovery Rates')
    ax.set_xlabel('High Income Countries with Most Cases')
    ax.set_title('Recovery Rates for High Income Countries with the Most Covid-19 Cases')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    plt.xticks(rotation=45, ha="right")


    fig.tight_layout()
    fig.savefig("HighIncome-bar.png")
    plt.show()

def visualization2(json_dict):
    #SCATTERPLOT
    calculations_keys_list = list(json_dict.keys())
    calculations_values_list = list(json_dict.values())
    
    co_levels = []
    for i in calculations_values_list[5:71]:
        co_levels.append(float(i))


    rng = np.random.RandomState(0)
    x = rng.randn(66)
    y = co_levels
    colors = rng.rand(66)
    sizes = 100
    m = 0 
    b = float(calculations_values_list[71])
    #make average level line
    plt.plot(x, m*x + b)
    

    plt.scatter(x, y, c=colors, s=sizes, alpha=0.3, cmap='viridis')
    plt.title('Air Quality measured by CO Levels in Chinese Cities with Average Level')
    plt.ylabel('Carbon Monoxide (CO) Level µg/m³')
    plt.xlabel('Readings of Different Chinese Cities within the Open AQ Platform')

    plt.show()
    


def visualization3(json_dict):
    #Make Bar Graph
    calculations_keys_list = list(json_dict.keys())
    calculations_values_list = list(json_dict.values())

    labels = []
    for i in calculations_keys_list[:4]:
        labels.append(i[19:])
    vals = []
    for i in calculations_values_list[:4]:
        vals.append(float(i))

    x = np.arange(len(labels))
    width = 0.5

    fig, ax = plt.subplots()
    rects = ax.bar(labels, vals, width, color = 'red')

    ax.set_ylabel('Total Recovered')
    ax.set_xlabel('Income Level')
    ax.set_title('Total Covid-19 Recoveries by Country Income Level ')
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    plt.xticks(rotation=45, ha="right")


    fig.tight_layout()
    plt.show()


def main():
    cur, conn = setUpDatabase("FinalProject.db")

    covid_data = request_covid_data()
    setUpCovidDataTable(cur, conn)
    insert_covid_rows(covid_data, cur, conn)

    AQ_data = request_openAQ_data1()
    setUpCountryOpenAQTable(cur, conn)
    insert_AQCountries_rows(AQ_data, cur, conn)

    AQ_param_data = request_openAQ_data2()
    setUpAQParameterTable(cur, conn)
    insert_AQParameter_rows(AQ_param_data, cur, conn)

    country_data = request_url_countries()
    setUpCountryTable(cur, conn)
    insert_Country_rows(country_data, cur, conn)

    write_csv_calculation_data(get_income_countries("High income", cur, conn), get_income_countries("Upper middle income", cur, conn), get_income_countries("Lower middle income", cur, conn), get_income_countries("Low income", cur, conn), cur, conn)

    calculations_data = csv_to_json_converter('final_proj_calculations.csv', 'final_proj_calculations.json')

    visual1 = visualization1(calculations_data)
    visual2 = visualization2(calculations_data)
    visual3 = visualization3(calculations_data)




if __name__ == "__main__":
    main()
