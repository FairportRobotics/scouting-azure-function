import azure.functions as func
from azure.storage.blob import BlobServiceClient
import logging

logging.info('Connecting to blob storage.')
connection_string = "DefaultEndpointsProtocol=https;AccountName=scoutingdatadev;AccountKey=2TDRHB8enPBg98Gp34n3gXEaC1K2SKsNeZDDb1zv5rRCHTum9GHlIc17bkFHL/hi9TU4rHF9k6mR+AStW7b+fw==;EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(conn_str=connection_string)
container_name = "crescendo"

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="v1")
def v1(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )