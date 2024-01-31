import azure.functions as func
from azure.storage.blob import BlobServiceClient
import logging
import json
import pandas as pd
from azure.cosmos import CosmosClient
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

def _get(req, key):
    data = req.params.get(key)
    if not data:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            data = req_body.get(key)
    return data


@app.route(route="v1")
def v1(req: func.HttpRequest) -> func.HttpResponse:
    # Assumes the containers have been created
    container_name = "crescendo"

    logging.info('Parsing data.')
    # Read in the data
    data = _get(req, 'data')
    data_type = _get(req, 'type')

    if not data:
        # Return a "helpful" message
        return func.HttpResponse("Bummer!  No data sent to this endpoint.", status_code=200)
    if not data_type:
        # Return a "helpful" message
        return func.HttpResponse("Bummer!  No type sent to this endpoint.", status_code=200)
    
    if data_type == "match":
        message = handle_match_data(data, container_name)
    return func.HttpResponse(message)


def handle_match_data(data, container_name):
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

    logging.info('Connecting to blob storage.')
    blob_connection_string = os.environ["BLOB_STORAGE_CONNECTION_STRING"]
    blob_service_client = BlobServiceClient.from_connection_string(conn_str=blob_connection_string)
    
    # Read in the existing data
    logging.info('Read existing data.')
    container_client = blob_service_client.get_container_client(container=container_name) 
    with open(file="/tmp/existing.csv", mode="wb") as download_file:
        download_file.write(container_client.download_blob(f"{container_name}.csv").readall())
    existing_df = pd.read_csv("/tmp/existing.csv")
    # Drop any existing data with the same key
    existing_df = existing_df[existing_df["key"] != match_data["key"]]
    
    # Save the raw JSON data
    logging.info('Saving raw data locally.')
    raw_path = "/tmp/" + match_data["key"]+".json"
    with open(raw_path, "w") as f:
        f.write(data)
    
    # Save the data locally
    logging.info('Saving locally.')
    df = pd.DataFrame([match_data])
    df = pd.concat([existing_df, df])
    local_file_name = f"/tmp/{container_name}.csv"
    df.to_csv(local_file_name, index=False)
        
    # Transfer the local file to blob storage
    logging.info('Saving to blob storage.')
    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{container_name}.csv")
    with open(file=local_file_name, mode="rb") as blob_data:
        blob_client.upload_blob(blob_data, overwrite=True)
        
    # Save the raw JSON to blob storage
    blob_client = blob_service_client.get_blob_client(container="raw", blob=match_data["key"]+".json")
    with open(file=raw_path, mode="rb") as blob_data:
        blob_client.upload_blob(blob_data, overwrite=True)
    
    # Save to Cosmos db
    logging.info('Insert into Cosmos db.')
    cosmos_client = CosmosClient(os.environ["COSMOS_URI"], credential=os.environ["COSMOS_KEY"])
    cosmos_database = cosmos_client.get_database_client(database=container_name)
    container = cosmos_database.get_container_client("match")
    # Add an id to the dictionary
    match_data["id"] = match_data["key"]
    # Insert into cosmos
    container.upsert_item(match_data)

    # Indicate our successful save
    return "Data synced to the cloud!"
        