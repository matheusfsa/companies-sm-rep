from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
# Definindo alguns argumentos básicos
default_args = {
   'owner': 'matheus',
   'depends_on_past': False,
   'start_date': datetime(2021, 11, 27),
   'retries': 0,
   }
   # Nomeando a DAG e definindo quando ela vai ser executada (você pode usar argumentos em Crontab também caso queira que a DAG execute por exemplo todos os dias as 8 da manhã)
   

with DAG(
   'twitter-dag',
   schedule_interval=timedelta(minutes=1),
   catchup=False,
   default_args=default_args
   ) as dag:
   # Definindo as tarefas que a DAG vai executar, nesse caso a execução de dois programas Python, chamando sua execução por comandos bash
   t1 = BashOperator(
            task_id='first_extraction',
            bash_command="""
            cd /home/matheus/companies_sm_rep
            source venv/bin/activate
            python src/twitter/extract.py --credentials=conf/local/credentials.yml
            """)