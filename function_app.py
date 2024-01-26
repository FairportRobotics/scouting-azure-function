import azure.functions as func
from azure.storage.blob import BlobServiceClient
import logging
import json

logging.info('Connecting to blob storage.')
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
    
        # TODO: Save data to crescendo.csv

        return func.HttpResponse(f"{match_data}")
    else:
        # Return a "helpful" message
        return func.HttpResponse("Bummer!  No data sent to this endpoint.", status_code=200)