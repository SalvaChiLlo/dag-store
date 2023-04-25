from __future__ import annotations

import logging
import shutil
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

@dag(
        schedule_interval=None, 
        start_date=datetime(2023, 5, 1), 
        catchup=False, 
        tags=["Custom Job Executor"],
        params={}
)
def custom_job_executor_v1():

    @task()
    def clone():
        print("CLONING REPOSITORY")

    @task()
    def install_dependencies():
        print("INSTALLING DEPENDECIES")

    @task()
    def execute():
        print("EXECUTE JOB")

    @task()
    def save_results():
        print("SAVE RESULTS")

    clone() >> install_dependencies() >> execute() >> save_results()

job_executor = custom_job_executor_v1()
