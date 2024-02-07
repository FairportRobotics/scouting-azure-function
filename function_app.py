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
    game_name = "crescendo"
    existing_csv_path = "/tmp/existing.csv"

    logging.info("Parsing data.")
    # Read in the data
    data = _get(req, "data")
    data_type = _get(req, "type").lower()

    if not data_type:
        # Return a "helpful" message
        return func.HttpResponse(
            json.dumps({"message": "Bummer!  No type sent to this endpoint."}),
            mimetype="application/json",
            status_code=200,
        )

    if data_type in ["match", "pit"]:
        if not data:
            # Return a different "helpful" message
            return func.HttpResponse(
                json.dumps({"message": "Bummer!  No data sent to this endpoint."}),
                mimetype="application/json",
                status_code=200,
            )
    else:
        # Hmmm.  Don't know what type of data was sent
        return_data = {"message": "Error: Unknown data type sent to this endpoint!"}

    # Here's the "Happy Path" for the app
    #
    # We will read in the JSON data to a dicitionary.  We will save it as a
    # raw json file.  We will also upsert the data into a CSV.  Both are
    # saved locally so they can be saved to blob storage.  Then the data is
    # prepped to save in a Cosmos DB.  We need to add in an id to make Cosmos
    # happy.  Once all of this has successfully completed, we return a
    # success message, along with all of the keys so the scouting app can
    # indicate what data has been saved.

    # Read the JSON data into a dictionary
    data = json.loads(
        data
    )  # We assume the JSON is flat.  If it's nested this will fail

    # Get settings based on what type of data was sent to the app
    if data_type == "match":
        csv_name = f"{game_name}.csv"
        raw_json_blob_name = data["key"] + ".json"
    elif data_type == "pit":
        csv_name = f"{game_name}_pit.csv"
        raw_json_blob_name = "pit_" + data["key"] + ".json"
    # These settings are generalizable
    local_file_path = f"/tmp/{csv_name}"
    raw_json_path = f"/tmp/{raw_json_blob_name}"
    cosmos_container = data_type

    # The Azure function app has the blob storage connection string saved as an
    # environmental variable.  We will use it to connect to blob storage.  We
    # assume that all data will be saved to a container matching the year's
    # game name and that this container has been previously created.
    logging.info("Connecting to blob storage.")
    blob_service_client = BlobServiceClient.from_connection_string(
        conn_str=os.environ["BLOB_STORAGE_CONNECTION_STRING"]
    )
    container_client = blob_service_client.get_container_client(container=game_name)

    # Save the raw JSON data
    logging.info("Saving raw JSON data locally.")
    with open(raw_json_path, "w") as f:
        f.write(json.dumps(data))

    # Save the raw JSON to blob storage
    logging.info("Saving the raw JSON to blob storage.")
    blob_client = blob_service_client.get_blob_client(
        container="raw", blob=raw_json_blob_name
    )
    with open(file=raw_json_path, mode="rb") as blob_data:
        blob_client.upload_blob(blob_data, overwrite=True)

    # Create a data frame for this reccord to upsert into the CSV
    df = pd.DataFrame([data])
    # TODO: Check if the existing data exists in the blob storage
    # Read in the existing data
    logging.info("Reading existing data.")
    with open(file=existing_csv_path, mode="wb") as download_file:
        download_file.write(container_client.download_blob(csv_name).readall())
    existing_df = pd.read_csv(existing_csv_path)
    # Drop any existing data with the same key
    existing_df = existing_df[existing_df["key"] != data["key"]]
    # Add the new data to the existing data (this is the upsert)
    df = pd.concat([existing_df, df])

    # Save the CSV data locally
    logging.info("Saving CSV data locally.")
    df.to_csv(local_file_path, index=False)

    # Transfer the local file to blob storage
    logging.info("Saving CSV to blob storage.")
    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(
        container=game_name, blob=csv_name
    )
    with open(file=local_file_path, mode="rb") as blob_data:
        blob_client.upload_blob(blob_data, overwrite=True)

    # Save to Cosmos db
    logging.info("Upsert into Cosmos db.")
    # Add an id to the dictionary to make Cosmos happy
    data["id"] = data["key"]
    cosmos_client = CosmosClient(
        os.environ["COSMOS_URI"], credential=os.environ["COSMOS_KEY"]
    )
    cosmos_database = cosmos_client.get_database_client(database=game_name)
    container = cosmos_database.get_container_client(cosmos_container)
    # Insert into cosmos
    container.upsert_item(data)

    # Indicate our successful save
    return_data = df[df.eventKey == data["eventKey"]]["key"].tolist()
    return func.HttpResponse(
        json.dumps({"message": "Data synced to the cloud!", "data_for": return_data}),
        mimetype="application/json",
        status_code=200,
    )
