import logging
import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        file = req.files.get('file')
        logging.info(f'file upload {file.filename}')
        conn_str = os.environ["arthaccapoc2_STORAGE"]
        container = "cca-edi-upload"
 
        with BlobServiceClient.from_connection_string(conn_str) as blob_service_client:
            with blob_service_client.get_blob_client(container=container, blob=file.filename) as blob_client:
                blob_client.upload_blob(file)

        # blobOut.set(file.read())
        # blobOut: func.Out[bytes]

    except Exception as err:
        logging.error(err.args)
        return func.HttpResponse(f"the file {file.filename} upload failed")

    return func.HttpResponse(f"the file {file.filename} upload successfully")

"""     name = req.params.get('name')
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
 """