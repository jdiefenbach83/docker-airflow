from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import boto3
import os
import pymongo
from sqlalchemy import create_engine

base_path = "/usr/local/airflow/data/"

# Função que faz o tratamento dos tweets
def tweet_para_df(tweet):
    try:
        df_tratado = pd.DataFrame(tweet).reset_index(drop=True).iloc[:1]
        df_tratado.drop(columns=['quote_count', 'reply_count', 'retweet_count', 'favorite_count',
                             'favorited', 'retweeted', 'user', 'entities', 'retweeted_status',
                            ], inplace=True)
        df_tratado['user_id']              = tweet['user']['id']
        df_tratado['user_id_str']          = tweet['user']['id_str']
        df_tratado['user_screen_name']     = tweet['user']['screen_name']
        df_tratado['user_location']        = tweet['user']['location']
        df_tratado['user_description']     = tweet['user']['description']
        df_tratado['user_protected']       = tweet['user']['protected']
        df_tratado['user_verified']        = tweet['user']['verified']
        df_tratado['user_followers_count'] = tweet['user']['followers_count']
        df_tratado['user_friends_count']   = tweet['user']['friends_count']
        df_tratado['user_created_at']      = tweet['user']['created_at']

        user_mentions = []

        for i in range(len(tweet['entities']['user_mentions'])):
            dicionariobase = tweet['entities']['user_mentions'][i].copy()
            dicionariobase.pop('indices', None)
            df = pd.DataFrame(dicionariobase, index=[0])
            df = df.rename(columns={
                'screen_name': 'entities_screen_name',
                'name' : 'entities_name',
                'id' : 'entities_id',
                'id_str' : 'entities_id_str'
            })
            user_mentions.append(df)

        dfs = []
        for i in user_mentions:
            dfs.append(
                pd.concat([df_tratado.copy(), i], axis=1)
            )
        df_final = pd.concat(dfs, ignore_index=True)
    except:
        return None
    return df_final


# Usando a novíssima Taskflow API
default_args = {
    'owner': 'Neylson Crepalde',
    "depends_on_past": False,
    "start_date": datetime(2021, 3, 17, 12),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False
}

@dag(default_args=default_args, schedule_interval=None, description="Teste de ETL de dados do Twitter")
def teste_dag_twitter():

    @task
    def start():
        print("Start!")
        return True
    
    @task
    def read_data_export_json(retorno):
        with open(base_path + 'collected_tweets_2021-03-17-16-34-17.txt', 'r') as file:
            tweets = file.readlines()
        
        for i in range(len(tweets)):
            # Fazendo o parse do tweet para um formato json e exporta arquivo
            with open(f'{base_path}tweet_{i}.json', 'w') as f:
                json.dump(json.loads( json.loads( tweets[i] ) ), f)
        return len(tweets) - 1
    

    @task
    def read_json_export_pandas(retorno):
        arquivos = [file for file in os.listdir(base_path) if file.startswith("tweet_")]
        print(arquivos)
        
        for arquivo in arquivos:
            with open(base_path + arquivo) as f:
                tweet = f.readlines()

            parsedtweet = json.loads(tweet[0])
            processado = tweet_para_df(parsedtweet)
            if processado is None:
                pass
            else:
                processado.to_csv(base_path + arquivo[:-4]+".csv", sep=';', index=False)
        return True
    
    @ task
    def concatenate_all_csvs(lista_retornos):
        arquivos = [f for f in os.listdir(base_path) if f.endswith(".csv")]
        dataframes = [pd.read_csv(base_path + arquivo, sep=";") for arquivo in arquivos]
        unico = pd.concat(dataframes, ignore_index=True)
        unico.to_csv(f"{base_path}tweets_dataframe_unico.csv", sep=";", index=False)
        return True


    st     = start()
    ntweets = read_data_export_json(st)
    check_file = FileSensor(task_id="check_file", filepath=f"{base_path}tweet_{ntweets}.json", poke_interval=10)
    res = read_json_export_pandas(ntweets)
    unico = concatenate_all_csvs(res)

    ntweets >> check_file >> res

execucao = teste_dag_twitter()

