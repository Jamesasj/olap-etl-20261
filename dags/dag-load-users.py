import requests
import pandas
import datetime
from airflow.sdk import dag
from airflow.sdk import task

@dag(
    start_date=datetime.datetime(2026, 3, 31), 
    schedule="@daily",
    tags=["james-sample"])
def my_sample_dag():
    
    @task
    def obter_usuarios():
        r = requests.get('https://jsonplaceholder.typicode.com/users')
        print('executed task 1')
        return r.json()

    @task
    def do_my_task_2(users):
        print('do_my_task_2 executed')

    @task
    def do_my_task_3():
        print('faz alguma coisa')

    my_task_1 = obter_usuarios()
    my_task_2 = do_my_task_2(my_task_1)
    my_task_3 = do_my_task_3()

    my_task_1 >> my_task_2 
    my_task_1 >> my_task_3


my_sample_dag()