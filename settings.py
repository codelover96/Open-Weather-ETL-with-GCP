import os
from dotenv import load_dotenv

load_dotenv()

is_local_environment = True  # set to false before deploy to GCP

cwd = os.getcwd()
keys_dir = os.path.join(cwd, "keys")

cloud_storage_service_account = str(os.path.join(keys_dir, os.environ["GCP_STORAGE_SERVICE_ACCOUNT_KEY"]))
bigquery_service_account = str(os.path.join(keys_dir, os.environ["GCP_BIG_Q_SERVICE_ACCOUNT_KEY"]))

api_key = os.environ["API_KEY"]
project_id = os.environ["PROJECT_ID"]
bucket_name = os.environ["BUCKET_NAME"]
dataset_id = os.environ["DATASET_ID"]

print(os.environ["CLOUD_FUNCTION_URL"])
