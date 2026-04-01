import requests
import pandas as pd
import datetime
from airflow.sdk import dag
from airflow.sdk import task
import json
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

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
    def remover_campo_ruims(users):
        data_frame = pd.read_json(json.dumps(users))
        selected_df = data_frame[['id', 'name' , 'email', 'phone', 'username' ]]
        return selected_df


    create_user_table = SQLExecuteQueryOperator(
        task_id="create_user_table",
        conn_id='my-conn' ,
        sql="""
            CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            username VARCHAR);
          """,
        )
    
    @task
    def dataframe_to_list(usuarios):
        return usuarios.values.tolist()

    @task
    def insert_list_into_table(rows):
        from airflow.providers.common.sql.hooks.sql import DbApiHook
        hook = DbApiHook.get_hook(conn_id="my-conn")
        hook.insert_rows(
            table="public.users",
            rows=rows,
            target_fields=["id", "name", "email", "phone", "username"],
            commit_every=1000,
        )

    my_task_1 = obter_usuarios()
    my_task_2 = remover_campo_ruims(my_task_1)
    my_task_3 = dataframe_to_list(my_task_2)
    insert_task = insert_list_into_table(my_task_3)

    my_task_1 >> my_task_2 
    my_task_1 >> create_user_table
    [create_user_table,my_task_2] >> my_task_3
    my_task_3 >> insert_task


my_sample_dag()