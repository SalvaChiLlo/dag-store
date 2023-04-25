from __future__ import annotations

import logging
import shutil
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger(__name__)


if not shutil.which("virtualenv"):
    log.warning(
        "The custom_job_executor DAG requires virtualenv, please install it.")
else:

    @dag(
            schedule_interval=None, 
            start_date=datetime(2022, 5, 1), 
            catchup=False, 
            tags=["Custom Job Executor"],
            params={
                'job_dir': '/tmp/{{ dag_run.run_id }}'
            }
    )
    def custom_job_executor():

        @task.virtualenv(
                requirements=["gitpython==3.1.31"]
        )
        def clone(git_url: str):
            from git import Repo
            print("CLONING REPOSITORY")

            if git_url == None or git_url == "":
                raise ValueError("You should provide a 'git_url'")
            
            Repo.clone_from(git_url, '{{ params.job_dir }}')
            print('{{ params.job_dir }}')

        @task()
        def install_dependencies():
            print("INSTALLING DEPENDECIES")

        @task()
        def execute():
            print("EXECUTE JOB")

        @task()
        def save_results():
            print("SAVE RESULTS")

        @task()
        def clean_environment():
            print("CLEAN ENVIRONMENT")


        clone('{{ dag_run.conf["git_url"] }}') >> install_dependencies() >> execute() >> save_results() >> clean_environment()

job_executor = custom_job_executor()
