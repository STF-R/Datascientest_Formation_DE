# !pip3 install GoogleNews
# !pip install newspaper3k
import numpy as np
import pandas as pd
import datetime
import time
from datetime import datetime, timedelta
import json
import os
from pytrends.request import TrendReq
from GoogleNews import GoogleNews
from newspaper import Article
# L'objet DAG nous sert à instancier notre séquence de tâches.
from airflow import DAG
# On importe les Operators dont nous avons besoin.
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python import PythonOperator
from tempfile import NamedTemporaryFile
from airflow.utils.dates import days_ago
# Les arguments qui suivent vont être attribués à chaque Operators.
# Il est bien évidemment possible de changer les arguments spécifiquement pour un Operators.
# Vous pouvez vous renseigner sur la Doc d'Airflow des différents paramètres que l'on peut définir.
default_args = {
    'owner': 'STF-R',
    'email': ['data.airflow@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
# Création du DAG
dag = DAG(
    '01_airflow_exo_STF-R',
    default_args=default_args,
    description='Airflow exo STF-R',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['exercice'],
)
####Le premier Operator (Extract) nous permet d'extraire des données de googlenews pour une
#####requête particulière ("query") et de stocker ces données dans un fichier json.
def extract(query, **kwargs):
    #set end/start = today/yesterday
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    start = str(now.month)+"/"+str(now.day)+"/"+str(now.year)
    end = str(yesterday.month)+"/"+str(yesterday.day)+"/"+str(yesterday.year)
    #googlenews result
    googlenews = GoogleNews(start=start,end=end)
    googlenews.search(query)
    result = googlenews.result()
    #set path to save json
    work_dir = os.getcwd()
    path1 = 'airflow'
    path2 = 'cellar'
    file_name = 'news-'+str(query)+'-'+str(now.year)+"-"+str(now.month)+"-"+str(now.day)
    #save json
    with open(os.path.join(work_dir, path1, path2, file_name+'.json'), 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=4, default=str)
t1 = PythonOperator(
        task_id='extract',
        python_callable=extract,
        op_kwargs={'query': 'bitcoin'},
        dag=dag,
    )
####Le deuxième Operator (Transform) nous permet de convertir notre fichier json en csv
#####en gardant seulement les url et les résumés des articles qui nous intéressent.
def transform(filename,**kwargs):
    time.sleep(10)
    with open(os.path.join(work_dir ,path1, path2, file_name+'.json')) as json_file:
        data = json.load(json_file)
    df = pd.DataFrame(data)
    url_list = []
    summary_list = []
    for url in df['link']:
        try:
            article = Article(url)
            article.download()
            article.parse()
            article.nlp()
            summary = article.summary
            url_list.append(url)
            summary_list.append(summary)
        except:
            pass
    df_summary = pd.DataFrame({'url':url_list, 'summary':summary_list})
    df_summary.to_csv(os.path.join(work_dir,path,file_name+'.csv'))
t2 = PythonOperator(
        task_id='transform',
        python_callable=transform,
        op_kwargs={'filename': os.path.join(work_dir, path1, path2, ,file_name+'.json')},
        dag=dag,
    )
####Le troisième Operator (build_email) nous permet d'envoyer notre jeu de données par mail.
def build_email(**context):
    time.sleep(5)
    df = read_csv(os.path.join(work_dir, path1, path2, file_name+'.csv'))
    df['to_email'] = df['url']+'\n'+df['summary']+'\n\n'
    numpy_array = df['to_email'].to_numpy()
    np.savetxt("./temp_mail.txt", numpy_array, fmt = "%s")
    email_op = EmailOperator(
        task_id='send_email',
        to="data.airflow@gmail.com",
        subject="Bitcoin news of the day",
        html_content=None,
        files="./temp_mail.txt",
    )
    email_op.execute(context)
t3 = email_op_python = PythonOperator(
    task_id="python_send_email", python_callable=build_email, provide_context=True, dag=dag
)

t1 >> t2 >> t3
