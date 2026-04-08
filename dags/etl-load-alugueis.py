import datetime
from airflow.sdk import dag
from airflow.sdk import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.hooks.sql import DbApiHook

def _cursor_to_dataframe(cursor):
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in rows]

@dag(
    start_date=datetime.datetime(2026, 3, 31), 
    schedule="@daily",
    tags=["james-sample"])
def load_locadora():
    create_cliente_table = SQLExecuteQueryOperator(
        task_id="create_cliente_table",
        conn_id='my-conn' ,
        sql='sql/tbl_stg_clientes.sql',
    )
    
    create_filme_table = SQLExecuteQueryOperator(
        task_id="create_filme_table",
        conn_id='my-conn' ,
        sql='sql/tbl_stg_filmes.sql',
    )

    create_midia_table = SQLExecuteQueryOperator(
        task_id="create_midia_table",
        conn_id='my-conn' ,
        sql='sql/tbl_stg_midias.sql',
    )

    create_alugueis_table = SQLExecuteQueryOperator(
        task_id="create_alugueis_table",
        conn_id='my-conn' ,
        sql='sql/tbl_stg_alugueis.sql',
    )
    # conectar e copiar (raw) do OLTP > STG (Extract)
    read_clientes = SQLExecuteQueryOperator(
        task_id="read_clientes",
        conn_id='oltp-conn' ,
        sql='sql/fetch_cliente.sql',
        handler=_cursor_to_dataframe,
        return_last=True
    )

    read_filmes = SQLExecuteQueryOperator(
        task_id="read_filmes",
        conn_id='oltp-conn' ,
        sql='sql/fetch_filme.sql',
        handler=_cursor_to_dataframe,
        return_last=True
    )

    read_midias = SQLExecuteQueryOperator(
        task_id="read_midias",
        conn_id='oltp-conn' ,
        sql='sql/fetch_midia.sql',
        handler=_cursor_to_dataframe,
        return_last=True
    )

    read_alugueis = SQLExecuteQueryOperator(
        task_id="read_alugueis",
        conn_id='oltp-conn' ,
        sql='sql/fetch_alugueis.sql',
        handler=_cursor_to_dataframe,
        return_last=True
    )

    @task
    def dload_cliente(df_clientes):
        print("Carregando clientes...")
        hook = DbApiHook.get_hook(conn_id="my-conn")
        hook.insert_rows(
            table="public.stg_clientes",
            rows=df_clientes,
            target_fields=["num_cliente", "nome", "endereco", "foneres", "fonecel"],
            commit_every=1000,
        )

    @task
    def dload_filme(df_filmes):
        print("Carregando filmes...")
        print(df_filmes)  # Access the DataFrame output

    @task
    def dload_midia(df_midias):
        print("Carregando midias...")
        print(df_midias)  # Access the DataFrame output

    @task
    def dload_emprestimo(df_emprestimos):
        print("Carregando emprestimos...")
        print(df_emprestimos)  # Access the DataFrame output

    end = EmptyOperator(task_id='end_workflow', trigger_rule='all_success') 
    
    dload_clientes_task = dload_cliente(read_clientes.output)
    dload_filmes_task = dload_filme(read_filmes.output)
    dload_midias_task = dload_midia(read_midias.output)
    dload_emprestimos_task = dload_emprestimo(read_alugueis.output)

    create_cliente_table >> read_clientes >> dload_clientes_task >> end
    create_filme_table >> read_filmes >> dload_filmes_task >> end
    create_midia_table >> read_midias >> dload_midias_task >> end
    create_alugueis_table >> read_alugueis >> dload_emprestimos_task >> end

load_locadora()
