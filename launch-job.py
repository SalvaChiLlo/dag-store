from __future__ import annotations

import logging
import shutil
import subprocess
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
        params={}
    )
    def custom_job_executor():

        @task.virtualenv(
            requirements=["gitpython==3.1.31"]
        )
        def clone(git_url: str, job_id: str):
            from git import Repo
            print("CLONING REPOSITORY")

            if git_url == None or git_url == "":
                raise ValueError("You should provide a 'git_url'")

            if job_id == None or job_id == "":
                raise ValueError("You should provide a 'job_id'")

            job_dir = f'/home/airflow/sources/logs/{job_id}'
            print(job_dir)
            Repo.clone_from(git_url, job_dir)

        @task()
        def install_dependencies(job_id: str):
            job_dir = f'/home/airflow/sources/logs/{job_id}'
            print("INSTALLING DEPENDECIES")
            with open(job_dir + '/output_dependencies.txt') as f:
                subprocess.Popen(
                    ['cd', job_dir, '&&', 'python install -r requirements.txt -t .'], stdout=f, stderr=f, universal_newlines=True)

        @task()
        def execute(job_id: str):
            print("EXECUTE JOB")
            job_dir = f'/home/airflow/sources/logs/{job_id}'
            with open(job_dir + '/output_execution.txt') as f:
                subprocess.Popen(
                    ['cd', job_dir, '&&', 'python main.py'], stdout=f, stderr=f, universal_newlines=True)

        @task()
        def save_results(job_id: str):
            print("SAVE RESULTS")

        @task()
        def clean_environment(job_id: str):
            print("CLEAN ENVIRONMENT")

        job_id = '{{ dag_run.conf["job_id"] }}'
        git_url = '{{ dag_run.conf["git_url"] }}'

        clone(git_url, job_id) >> install_dependencies(job_id) >> execute(
            job_id) >> save_results(job_id) >> clean_environment(job_id)

job_executor = custom_job_executor()
