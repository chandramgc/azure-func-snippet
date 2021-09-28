import logging
import json
import pandas as pd
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings, ContainerClient
from datetime import datetime
import os
import uuid
import tempfile

def build_current_node(elements: str, conf: str):
    data = {}
    data_element = {}
    for index, element in enumerate(elements,0):
        if (str(index) in conf):
            conf_current_node = conf[str(index)]
            conf_current_element = conf_current_node['Value']
            if (index == 0):
                data[element] = {}
            else:
                if ("Options" in conf_current_node):
                    if (element in conf_current_node['Options']):
                        options_results = dict(conf_current_node['Options'])
                        data_element[conf_current_element] = options_results.get(element)
                else:
                    data_element[conf_current_element] = element.strip()
        
        #print(f'{index} {element} {conf_current_element}')
    data[elements[0]] = data_element
    #print(data_element)
    #print(json.dumps(data, indent = 4) )
    return elements[0], data_element

def convert_edi_data_2_json(file_data: str,conf_data: str):
    data = {}
    
    segments = file_data.split('~')
    for segment in segments:
        elements = segment.split('*')
        # print(elements[0])
        if elements[0] in conf_data:
            conf_filter = conf_data[str(elements[0])]
            current_element, current_payload = build_current_node(elements, conf_filter)
            data[current_element] = current_payload
            #print(current_node)
    return data

def convert_json_2_csv(json_data: dict,conf_data: str):
    conf_mapping = dict(conf_data['MAPPING'])
    #print(conf_mapping)
    elements = pd.json_normalize(json_data)
    elements.columns = elements.columns.str.split('.').str[-1]
    result_df= pd.DataFrame()
    for map in conf_mapping.items():
        if map[0] in elements.columns:
            #print(f'{map[0]=}:{map[1]=} = {elements[str(map[0])].iloc[0]=}')
            result_df[map[1]] = elements[str(map[0])]
    #print(result_df.to_markdown())
    return result_df

def main(message: func.ServiceBusMessage):
    # Log the Service Bus Message as plaintext

    message_content_type = message.content_type
    message_body = message.get_body().decode("utf-8")
    # Get Application settings
    conn_str = os.environ["arthaccapoc2_STORAGE"]

    container_config = "cca-edi-config"
    container_extract = "cca-edi-extract"
    edi_config = "edi_810_config_min.json"

    
    #blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    #blob_client = blob_service_client.get_blob_client(container=container, blob=file.filename)
    #blob_client.upload_blob(file)

    with BlobServiceClient.from_connection_string(conn_str) as blob_service_client:
        with blob_service_client.get_blob_client(container=container_config, blob=edi_config) as blob_client:            
            download_stream = blob_client.download_blob()
            config_body = json.loads(download_stream.readall().decode('utf-8'))
            #print(config_body)
            payload = convert_edi_data_2_json(message_body,config_body)
            payload_csv = convert_json_2_csv(payload,config_body)
            #print(payload_csv.to_markdown())
            local_file_name = str(uuid.uuid4()) + ".txt"
        with blob_service_client.get_blob_client(container=container_extract, blob=local_file_name) as blob_client:
            
    
            now = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")    
            edi_file_name = "edi_" + now
            #blob_client.upload_blob(payload_csv.to_string(), blob_type="AppendBlob")
            # payload_csv.to_csv(edi_file_name,sep=',')
            # image_content_setting = ContentSettings(content_type='text/plain')
            # print(f"uploading file - {edi_file_name}")
            # with open("./"+edi_file_name, "rb") as data:
            #   blob_client.upload_blob(data,overwrite=True,content_settings=image_content_setting)             
            # print(type(payload_csv.to_csv(edi_file_name,sep=',')))   
            # print(payload_csv.to_csv(edi_file_name,sep=','))

            # Create a local directory to hold blob data
            
            local_path = tempfile.gettempdir()
            isdir = os.path.isdir(local_path)
            if not isdir:
                os.mkdir(local_path)

            # Create a file in the local data directory to upload and download
            
            upload_file_path = os.path.join(local_path, local_file_name)

            # Write text to the file
            file = open(upload_file_path, 'w')
            file.write(payload_csv.to_string())
            file.close()

            # Create a blob client using the local file name as the name for the blob
            blob_client = blob_service_client.get_blob_client(container=container_extract, blob=local_file_name)

            print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

            # Upload the created file
            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data)
        

    logging.info("Python ServiceBus topic trigger processed message.")
    logging.info("Message Content Type: " + message_content_type)
    logging.info("Message Body: " + message_body)

    