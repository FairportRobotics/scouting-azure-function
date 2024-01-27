import azure.functions as func
from azure.storage.blob import BlobServiceClient
import logging
import json
import pandas as pd

logging.info('Connecting to blob storage.')
# TODO: Store the connection_string in a more secure location than the source code
connection_string = "DefaultEndpointsProtocol=https;AccountName=scoutingdatadev;AccountKey=2TDRHB8enPBg98Gp34n3gXEaC1K2SKsNeZDDb1zv5rRCHTum9GHlIc17bkFHL/hi9TU4rHF9k6mR+AStW7b+fw==;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)
container_name = "crescendo"

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="v1")
def v1(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Parsing data.')
    # Read in the data
    data = req.params.get('data')
    if not data:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            data = req_body.get('data')

    if data:
        # Read the JSON data
        j = json.loads(data)
        # Flatten the JSON data
        logging.info('Flattening JSON data.')
        match_data = {}
        for key,value in j.items():
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    match_data[key + "_" + sub_key] = sub_value
            else:
                match_data[key] = value
        
        # Save the raw JSON data
        logging.info('Saving raw data locally.')
        raw_path = "/tmp/" + match_data["key"]+".json"
        with open(raw_path, "w") as f:
            f.write(data)

        # Save the data locally
        logging.info('Saving locally.')
        df = pd.DataFrame([match_data])
        local_file_name = "/tmp/crescendo.csv"
        df.to_csv(local_file_name, index=False)
        
        # Transfer the local file to blob storage
        logging.info('Saving to blob storage.')
        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob="crescendo.csv")
        with open(file=local_file_name, mode="rb") as blob_data:
            blob_client.upload_blob(blob_data)
        
        # Save the raw JSON to blob storage
        blob_client = blob_service_client.get_blob_client(container="raw", blob=match_data["key"]+".json")
        with open(file=raw_path, mode="rb") as blob_data:
            blob_client.upload_blob(blob_data)
        
        # Indicate our successful save
        return func.HttpResponse(f"Data synced to the cloud!")
    else:
        # Return a "helpful" message
        return func.HttpResponse("Bummer!  No data sent to this endpoint.", status_code=200)