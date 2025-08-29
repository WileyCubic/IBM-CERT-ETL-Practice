# ETL Script to extract data from csv, json, xml files, transform it and load to a csv file
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import glob
from datetime import datetime

# File paths for log and output
log_file = 'log_file.txt'
target_file = 'transformed_data.csv'

# Extract data from all files to dataframe
files = glob.glob('*.csv') + glob.glob('*.json') + glob.glob('*.xml')

# Function for extracting data from xml, csv, json

def extract_data(file):
    dataframe = pd.DataFrame(columns=['name', 'height', 'weight'])
    
    for csv_file in glob.glob('*.csv'):
        if csv_file != target_file:
            dataframe = pd.concat([dataframe, pd.read_csv(csv_file)], ignore_index=True)
            print(f"CSV file {csv_file} has been extracted.")
    for json_file in glob.glob('*.json'):
        dataframe = pd.concat([dataframe, pd.read_json(json_file, lines=True)], ignore_index=True)
        print(f"JSON file {json_file} has been extracted.")
    for xml_file in glob.glob('*.xml'):
        dataframe = pd.concat([dataframe, pd.read_xml(xml_file, parser='etree')], ignore_index=True)
        print(f"XML file {xml_file} has been extracted.")
    return dataframe

# Extracted dat is in inches and pounds
# Transform into meters and kilograms rounded to 2 decimal places

def transform(data):
    data['height'] = round(data['height'] * 0.0254, 2)
    data['weight'] = round(data['weight'] * 0.453592, 2)
    return data

# Load transformed data to csv file

def load_data(data, export_file):
    data.to_csv(export_file)

# define a logging function

def log(message):
    timestamp_format = '%Y-%m-%d %H:%M:%S'  # Year-Month-Day Hour:Minute:Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(f"{timestamp}, {message}\n")


# Log the initialization of the ETL process
log("ETL Job Started")

# Log the begining of the Extraction process
log("Extract phase Started")
extracted_data = extract_data(files)

# Log the end of the Extraction process
log("Extract phase Ended")

# Log the begining of the Transformation process
log("Transform phase Started")
transformed_data = transform(extracted_data)
print('Trasnformed data')
print(transformed_data)

# Log the conpletion of the Transformation process
log("Transform phase Ended")

# Log the begining of the loading process
log;("Load phase Started")
load_data(transformed_data, target_file)

# Log the completion of the loading process
log("Load phase Ended")

# Log the completion of the ELT process
log("ETL Job Ended")
 





