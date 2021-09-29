from airflow.decorators import dag, task
from datetime import datetime
from random import random

default_args = {
    'owner': 'Neylson',
    'start_date': datetime(2021, 5, 25)
}


@dag(default_args=default_args, schedule_interval="/2 * * * *", description="Dag do GUDS")
def dag_guds():


    @task
    def start():
        print("Foi dada a largada!!!! Uhuuuullll")

    @task
    def gera_num_aleatorio():
        return random()


    @task
    def define_quanto_legal(num):
        print(f"O Neylson é {num*100} legal!!")



    retorno1 = start()
    numrand = gera_num_aleatorio()
    resfinal = define_quanto_legal(numrand)


dag_guds()