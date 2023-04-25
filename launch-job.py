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

    @dag(schedule=None, start_date=datetime(2023, 5, 1), catchup=False, tags=["Custom Job Executor"])
    def custom_job_executor():

        @task.virtualenv(requirements=["gitpython==latest"])
        def clone(git_url: str, job_id: str):
            from git import Repo

            if git_url == None or git_url == "":
                raise ValueError("You should provide a 'git_url'")

            if job_id == None or job_id == "":
                raise ValueError("You should provide a 'job_id'")

            Repo.clone_from(git_url, f'/tmp/{job_id}')

        @task()
        def install_dependencies():

            return

        @task()
        def execute():

            return

        @task()
        def save_results():

            return

        clone() >> install_dependencies() >> execute() >> save_results()
custom_job_executor()