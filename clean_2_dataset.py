

import pandas as pd
from difflib import get_close_matches 

weather_data_path = 'C:/Users/k1332/MapReduce-House-Weather/total-weather-data.csv'
house_data_path = 'C:/Users/k1332/MapReduce-House-Weather/Planning_Application_Sites_2010_onwards.csv'


print('\n\n  *** Weather_ DATA ***  \n')
df_weather = pd.read_csv(weather_data_path)
print("length of weather data = ",len(df_weather))
print('number of columns = ',len(df_weather.columns))
print("weather columns : ",df_weather.columns)
print(df_weather.dtypes)
print(df_weather['date'].describe())

print('\n\n  *** HOUSE_PLANNING DATA ***  \n')
df_house = pd.read_csv(house_data_path)
print("length of house plan data = ", len(df_house))
print('number of columns = ',len(df_house.columns))
print('house columns : ',df_house.columns)
print('number of columns after remove the non valued columns  = ',len(df_house.columns))
print(df_house.dtypes)

index_list = []
date_list = []
for i in range(len(df_weather['date'])):
    temp = df_weather['date'][i].split('-')
    try:
        if 10 <= int(temp[2]) <= 19:
            index_list.append(i)
            year = '20' + str(temp[2])
            if str(temp[1]) == 'Jan':
                month = '01'
            elif str(temp[1]) == 'Feb':
                month = '02'
            elif str(temp[1]) == 'Mar':
                month = '03'
            elif str(temp[1]) == 'Apr':
                month = '04'
            elif str(temp[1]) == 'May':
                month = '05'
            elif str(temp[1]) == 'Jun':
                month = '06'
            elif str(temp[1]) == 'Jul':
                month = '07'
            elif str(temp[1]) == 'Aug':
                month = '08'
            elif str(temp[1]) == 'Sep':
                month = '09'
            elif str(temp[1]) == 'Oct':
                month = '10'
            elif str(temp[1]) == 'Nov':
                month = '11'
            elif str(temp[1]) == 'Dec':
                month = '12'
            date = year + '-' + month + '-' + str(temp[0])
            date_list.append(date)
    except:         
        pass
print('length of converted weather data  = ',len(index_list))

df_weather = df_weather.iloc[index_list , : ]
df_weather['converted-datetime'] = date_list
df_weather = df_weather.reset_index()
df_weather['converted-datetime'] =  pd.to_datetime(df_weather['converted-datetime'])
print(df_weather.head())

print('old : ',list(df_weather['county'].value_counts().keys()))
targets = ['mayo','dublin','donegal','cavan','wexford','clare','meath','tipperacy','kerry','roscommon','galway'
          ,'carlow','kildare','sligo','leitrim','louth','wicklow','kilkenny']
for i in range(len(df_weather['county'])):
    similarity = list(get_close_matches(str(df_weather['county'][i]).lower(), targets))
    if len(similarity) > 0:
        df_weather['county'][i] = similarity[0]
    if str(df_weather['county'][i]) == 'newport-mayo':
        df_weather['county'][i] = 'mayo'
    if str(df_weather['county'][i]) == 'MONAGHAN':
        df_weather['county'][i] = 'monaghan'
    if str(df_weather['county'][i]) == 'shannon-claire':
        df_weather['county'][i] = 'clare'
for i in range(len(df_weather['county'])):
    df_weather['county'][i] = df_weather['county'][i].title()

for i in range(len(df_house['PlanningAuthority'])):
    temp = str(df_house['PlanningAuthority'][i]).split(' ')
    try:
        name = ''
        for k in range(len(temp)-2):
            name += temp[k] 
        df_house['PlanningAuthority'][i] = name
    except:
        pass
    if df_house['PlanningAuthority'][i] == 'SouthDublin':
        df_house['PlanningAuthority'][i] = 'Dublin'
    elif df_house['PlanningAuthority'][i] == 'WaterfordCityand':
        df_house['PlanningAuthority'][i] = 'Waterford'
    elif df_house['PlanningAuthority'][i] == 'Westmeath':
        df_house['PlanningAuthority'][i] = 'Meath'

df_house['county'] = df_house['PlanningAuthority']


print('new: ',list(df_weather['county'].value_counts().keys()))
print('weather : ',len(list(df_weather['county'].value_counts().keys())))
print('\ncounties of House Data : ',list(df_house['PlanningAuthority'].value_counts().keys()))
print(len(list(df_house['county'].value_counts().keys())))

df_weather =  df_weather.sort_values(by =['county','converted-datetime'])
df_weather.to_csv('C:/Users/k1332/MapReduce-House-Weather/Converted-Weather-data.csv', index=False, encoding="utf-8")

df_house =  df_house.sort_values(by =['county','ReceivedDate'])
df_house.to_csv('C:/Users/k1332/MapReduce-House-Weather/Converted-House-data.csv', index=False, encoding="utf-8")


