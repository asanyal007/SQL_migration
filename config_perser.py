import configparser
import pandas as pd
parser = configparser.ConfigParser()
parser.read('conf/ETLConfig.ini')

gcp = parser['GCP']['PROJECT_ID']

json_file = parser['GCP']['OENVIRON']

for k,v in parser['SOURCE'].items():
    print(k,v)



