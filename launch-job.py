from __future__ import annotations

import logging
import shutil
from datetime import datetime

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

if not shutil.which("virtualenv"):
    log.warning("The custom_job_executor DAG requires virtualenv, please install it.")
else:

    @dag(schedule=None, start_date=datetime(2023, 5, 1), catchup=False, tags=["Custom Job Executor"])
    def custom_job_executor():

        @task.virtualenv(requirements=["gitpython==latest"])
        def clone():
            from git import Repo
            Repo.clone_from(params.git_url)

        @task()
        def install_dependencies():
            
            return

        @task()
        def execute():

            return

        @task()
        def save_results():
            
            return
        
        clone()
        install_dependencies()
        execute()
        save_results()

    custom_job_executor()