from sodapy import Socrata
import requests
from requests.auth import HTTPBasicAuth
import argparse
import sys
import os
import json
 
parser = argparse.ArgumentParser(description='Process data from parking violations.')
parser.add_argument('--page_size', type=int, help='how many rows to get per page', required=True)
parser.add_argument('--num_pages', type=int, help='how many pages to get in total')
args = parser.parse_args(sys.argv[1:])
print(args)

# Let’s comment hardcoded values out and create environment variables
#DATASET_ID="nc67-uf89"
#APP_TOKEN="bcL7B5CfP7xg7Vzk8kd0vho8F"
#ES_HOST="https://search-cis9760-saipavani-cheruku-azdlz2okyls4n226zikrcy5aua.us-east-1.es.amazonaws.com"
#ES_USERNAME="spcproj"
#ES_PASSWORD="SPCproj1$"
#INDEX_NAME ="parking01"

DATASET_ID=os.environ["DATASET_ID"]
APP_TOKEN=os.environ["APP_TOKEN"]
ES_HOST=os.environ["ES_HOST"]
ES_USERNAME=os.environ["ES_USERNAME"]
ES_PASSWORD=os.environ["ES_PASSWORD"]
INDEX_NAME=os.environ["INDEX_NAME"]


if __name__ == '__main__':
    
    try:
        # {ES_HOST}/{INDEX_NAME}: This is the URL to create payroll index, which is our Elasticsearch db.
        resp = requests.put(f"{ES_HOST}/{INDEX_NAME}", auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            json={
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                },
                "mappings": {
                    "properties": {
                        "state": {"type": "keyword"},
                        "summons_number": {"type": "keyword"},
                        "issue_date": {"type": "date", "format": "mm/dd/yyyy"},
                        "violation": {"type": "keyword"},
                        "fine_amount": {"type": "float"},
                        "precinct": {"type": "keyword"},
                        "issuing_agency": {"type": "keyword"},
                    }
                },
            }
        )
        resp.raise_for_status()
        #print(resp.json())

    # If try to run the above code again, which creates an index, it will give an error. Because, it’s already created.
    # In order to avoid it, we raise an exception.
    except Exception as e:
        print("Index already exists! Skipping")

    for i in range(0, args.num_pages):
        client = Socrata("data.cityofnewyork.us", APP_TOKEN,)
        rows = client.get(DATASET_ID, limit=args.page_size, offset=(i*args.page_size),)
        es_rows = [] 
        
        for row in rows:
            try:
                # Convert
                es_row = {}
                es_row["state"] = row["state"]
                es_row["summons_number"] = row["summons_number"]
                es_row["issue_date"] = row["issue_date"]
                es_row["violation"] = row["violation"]
                es_row["fine_amount"] = float(row["fine_amount"])
                es_row["precinct"] = row["precinct"]
                es_row["issuing_agency"] = row["issuing_agency"]
          
            except Exception as e:
                #print (f"Error!: {e}, skipping row: {row}")
                continue
            
            es_rows.append(es_row)
         
        #bulk API    
        bulk_upload_data = ""
        
        for line in es_rows:
            print(f'Handling row {line["summons_number"]}')
            action = '{"index": {"_index": "' + INDEX_NAME + '", "_type": "_doc", "_id": "' + line["summons_number"] + '"}}'
            data = json.dumps(line)
            bulk_upload_data += f"{action}\n"
            bulk_upload_data += f"{data}\n"
    
        #print (bulk_upload_data)
        
        try:
            # Upload to Elasticsearch by creating a document
            resp = requests.post(f"{ES_HOST}/_bulk",
                    data=bulk_upload_data,auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD), headers = {"Content-Type": "application/x-ndjson"})
            resp.raise_for_status()
            #print(resp.json())
            print("Done")
            # If it fails, skip that row and move on.
        except Exception as e:
            #print(f"Failed to insert in ES: {e}, skipping row: {row}")
            continue
        
    
        '''    
        try:
            # Upload to Elasticsearch by creating a document
            resp = requests.post(f"{ES_HOST}/{INDEX_NAME}/_doc",
                    json=es_row,auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),)
            resp.raise_for_status()
            
            # If it fails, skip that row and move on.
        except Exception as e:
            print(f"Failed to insert in ES: {e}, skipping row: {row}")
            continue
        
        #print(resp.json())
        '''
