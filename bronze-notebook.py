# Databricks notebook source
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd

# Azure Data Lake Storage details
storage_account_name = "your_storage_account_name"
storage_account_key = "your_storage_account_key"
file_system_name = "your_file_system_name"
directory_name = "your_directory_name"
file_name = "your_file_name.csv"

# Reading data from CSV
data = pd.read_csv("local_file.csv")

# Initialize Data Lake service client
try:
    service_client = DataLakeServiceClient(account_url=f"https://{storage_account_name}.dfs.core.windows.net", credential=storage_account_key)
    file_system_client = service_client.get_file_system_client(file_system=file_system_name)
    directory_client = file_system_client.get_directory_client(directory_name)
    
    file_client = directory_client.create_file(file_name)
    with file_client.create_file() as f:
        data.to_csv(f)
    print("Data successfully ingested into Azure Data Lake Storage!")
    
except Exception as e:
    print(f"Error: {e}")

