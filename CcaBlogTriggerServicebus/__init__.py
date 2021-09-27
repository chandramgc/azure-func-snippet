import logging
import azure.functions as func

def main(blobIn: func.InputStream, msgOut: func.Out[str]):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {blobIn.name}\n"
                 f"Blob Size: {blobIn.length} bytes")
    try:
        file_data = blobIn.read().decode('utf-8')

        # conn_string = "Endpoint=sb://blobtriggertest101.servicebus.windows.net/;SharedAccessKeyName=edi-policy;SharedAccessKey=DJgfzWHxCsEX+vyvJGV6Idv7MSC3o0leDjMVYObLU30="
        # queue_name = "cca-edi-topic"
        # servicebus_client = ServiceBusClient.from_connection_string(conn_str=conn_string, logging_enable=True)
        # with servicebus_client:
        #     sender = servicebus_client.get_queue_sender(queue_name=queue_name)
        #     with sender:
        #         sender.send_messages(file_data)
        #         print("Sent a single message")
        
        msgOut.set(file_data)
        logging.info(f"Msg sent to cca-edi-topic file {blobIn.name}")
       
        
    except Exception as e:
        print('Error:')
        print(e)
