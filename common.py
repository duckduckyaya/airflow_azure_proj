import os
from typing import Tuple
from azure.storage.blob import BlobServiceClient

def get_enviroment_variables() -> Tuple[str, str]:
    """
    return the environment variables if they are set. If not, raise an error.
    """

    connection_string = os.getenv("AZURE_CONNECTION_STRING")
    container_name = os.getenv("AZURE_CONTAINER_NAME")
    if not connection_string:
        raise ValueError("AZURE_CONNECTION_STRING not found. Check your .env file")
    elif not container_name:
        raise ValueError("AZURE_CONTAINER_NAME not found. Check your .env file")    

    print("Environment variables are set:")

    return connection_string, container_name