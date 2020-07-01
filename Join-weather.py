

#import required libraries
from os import chdir 
from glob import glob 
import pandas as pdlib

# Move to the path that holds our CSV files
csv_file_path = '/Users/atandaridwan/Desktop/DIA-Project'
chdir(csv_file_path)

print(csv_file_path)
# List all CSV files in the working dir
file_pattern = "csv"
list_of_files = [file for file in glob('*.{}'.format(file_pattern))]
print(list_of_files)

"""
 Function:
  Produce a single CSV after combining all files
"""

for file in list_of_files:
    print(file)
    name = str(file).replace('.csv','')
    name = ''.join([i for i in name if not i.isdigit()])
    target_pd = pdlib.read_csv(file)
    target_pd = target_pd.dropna(axis='columns')
    name_list = []
    for i in range(len(target_pd)): 
        name_list.append(name)
    target_pd['county'] = name_list
    target_pd.to_csv(file, index=False, encoding="utf-8")


def produceOneCSV(list_of_files, file_out):
   # Consolidate all CSV files into one object
   result_obj = pdlib.concat([pdlib.read_csv(file,error_bad_lines=False) for file in list_of_files],sort = False)
   # Convert the above object into a csv file and export
   result_obj.to_csv(file_out, index=False, encoding="utf-8")

file_out = "/Users/atandaridwan/Desktop/DIA-Project/total-weather-data.csv"

produceOneCSV(list_of_files, file_out)