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
        @dag(
            schedule_interval=None,
            start_date=datetime(2022, 5, 1),
            catchup=False,
            tags=["Custom Job Executor"],
            params={}
        )
        def my_job_executor():
            @task.virtualenv(
                requirements=["gitpython==3.1.31"]
            )
            def clone(git_url: str, job_id: str, job_dir: str):
                from git import Repo
                print("CLONING REPOSITORY")

                if git_url == None or git_url == "":
                    raise ValueError("You should provide a 'git_url'")

                if job_id == None or job_id == "":
                    raise ValueError("You should provide a 'job_id'")

                print(job_dir)
                Repo.clone_from(git_url, job_dir)

            @task()
            def install_dependencies(job_id: str, job_dir: str):
                time.sleep(10)
                print("INSTALLING DEPENDECIES")
                subprocess.Popen(
                    ['python', f'install -r {job_dir}/requirements.txt -t {job_dir}/'])

            @task()
            def execute(job_id: str, job_dir: str):
                print("EXECUTE JOB")
                subprocess.Popen(['python', f'{job_dir}/main.py'])

            @task()
            def save_results(job_id: str, job_dir: str):
                print("SAVE RESULTS")
                

            @task()
            def clean_environment(job_dir: str):
                print("CLEAN ENVIRONMENT")
                # subprocess.Popen(['rm', f'-rf {job_dir}/'])

            job_id = '{{ dag_run.conf["job_id"] }}'
            git_url = '{{ dag_run.conf["git_url"] }}'
            job_dir = f'/home/airflow/sources/logs/{job_id}'

            clone(git_url, job_id, job_dir) >> install_dependencies(job_id, job_dir) >> execute(
                job_id, job_dir) >> save_results(job_id, job_dir) >> clean_environment(job_dir)
        my_job_executor()
custom_job_executor()
