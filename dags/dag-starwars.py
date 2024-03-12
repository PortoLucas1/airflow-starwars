from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from airflow import DAG

from datetime import datetime 



@dag(
    dag_id='dag-starwars',
    start_date=datetime(2024,3,12),
    schedule=None
)
def dag_starwars():
    @task
    def my_task():
        print("oooi")
    
    my_task = my_task()
    
dag_starwars = dag_starwars()