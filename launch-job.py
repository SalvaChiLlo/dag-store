from __future__ import annotations

import logging
import shutil
import subprocess
import time
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
        def execute_job(git_url: str, job_id: str, job_dir: str):
            from git import Repo
            import subprocess

            def clone():
                print("CLONING REPOSITORY")

                if git_url == None or git_url == "":
                    raise ValueError("You should provide a 'git_url'")

                if job_id == None or job_id == "":
                    raise ValueError("You should provide a 'job_id'")

                print(job_dir)
                Repo.clone_from(git_url, job_dir)

            def install_dependencies():
                print("INSTALLING DEPENDECIES")
                subprocess.Popen(
                    ['pip', 'install', '-r', f'{job_dir}/requirements.txt', '-t', f'{job_dir}/', '--no-user'])

            def execute():
                print("EXECUTING JOB")
                subprocess.Popen(['python', f'{job_dir}/main.py'])

            def save_results():
                print("SAVING RESULTS")

            def clean_environment():
                print("CLEANING ENVIRONMENT")
                # subprocess.Popen(['rm', f'-rf {job_dir}/'])

            clone()
            install_dependencies()
            execute()
            save_results()
            clean_environment()

        job_id = '{{ dag_run.conf["job_id"] }}'
        git_url = '{{ dag_run.conf["git_url"] }}'
        job_dir = f'/home/airflow/sources/logs/{job_id}'

        execute_job(git_url, job_id, job_dir)
    custom_job_executor()
