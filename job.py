import copy
import os
import json
from pathlib import Path
import requests

def list_databricks_jobs(name, token, url):
    """
    This function lists all Databricks jobs and returns the job ID of the job with the specified name.

    Parameters:
    name (str): The name of the job to find.
    token (str): The token used for authentication.
    url (str): The base URL of the Databricks instance.

    Returns:
    int: The job ID of the job with the specified name. If no such job is found, it returns None.
    """
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    response = requests.get(f"https://{url}/api/2.1/jobs/list", headers=headers)
    main_job_id = None
    # If the response status code is 200, that means the request was successful.
    if response.status_code == 200:
        jobs = response.json()["jobs"]
        for job in jobs:
            if job["settings"]["name"] == name:
                main_job_id = job["job_id"]
    else:
        # If the response status code was anything other than 200, we print an error message.
        print(f"Failed to list jobs. Reason: {response.text}")

    return main_job_id

def update_raw_job():

    base_reset_file = os.path.join(Path(__file__).parent, "templates/base_reset.json")
    base_cluster_file = os.path.join(Path(__file__).parent, "templates/clusters.json")
    base_job_file = os.path.join(Path(__file__).parent, "templates/job_base.json")

    base_reset = json.loads(open(base_reset_file, "r").read())
    base_cluster = json.loads(open(base_cluster_file, "r").read())
    base_job = json.loads(open(base_job_file, "r").read())
    token = os.environ.get("DATABRICKS_TOKEN")
    url = os.environ.get("DATABRICKS_URL")

    # If no token is obtained, return
    if token is None:
        return

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    job = base_job.copy()

    job = job.replace("|LOCATION|", os.environ.get("LOCATION"))
    job = job.replace("|SCHEMA|", os.environ.get("SCHEMA"))
    job = job.replace("|CHECKPOINT|", os.environ.get("CHECKPOINT"))

    # Check if the job already exists
    job_id = list_databricks_jobs("MAIN", token, url)

    # Add the cluster configuration to the job
    job["job_clusters"] = base_cluster

    # If the job exists, prepare the reset configuration
    if job_id:
        final = copy.deepcopy(base_reset)
        final["new_settings"] = job
        final["job_id"] = job_id
    else:
        final = job

    # If the job exists, reset it. Otherwise, create a new job
    if job_id:
        response = requests.post(f"https://{url}/api/2.1/jobs/reset", headers=headers, data=json.dumps(final))
    else:
        response = requests.post(f"https://{url}/api/2.1/jobs/create", headers=headers, data=json.dumps(final))

    if response.status_code == 200:
        print(f"Job created. Job ID: {response.json()}")
        return True
    else:
        print(f"Failed to create job. Reason: {response.text}")
        return False
    
if __name__ == "__main__":
    update_raw_job()