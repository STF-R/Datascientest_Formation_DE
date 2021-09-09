import random
import pandas as pd
from flask import Flask
import json
from flask import jsonify
from flask import make_response
from flask import abort, request
import requests
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

##fonctions utiles
def perm_value(username, password):
    df_cred = pd.read_csv("./credentials.csv") #dataframe des credentials
    if any(df_cred['username']==str(username)) and any(df_cred['password']==int(password)):#le username et le password sont bien référencés ds les credentials
        indx_usr = df_cred.loc[(df_cred['username']==str(username))].index[0] #index de la ligne à laquelle se trouve le username recherché
        indx_pwd = df_cred.loc[(df_cred['password']==int(password))].index[0] #index de la ligne à laquelle se trouve le password recherché
        if indx_usr==indx_pwd:#le username et le password matchent
            v1_col, v2_col = 2,3 #index des colonnes v1 et v2 dans le dataframe
            indx = df_cred.loc[(df_cred['username']==str(username)) & (df_cred['password']==int(password))].index[0] #index de la ligne à laquelle se trouve la paire (username, password) recherchée
            v1 = df_cred.iloc[indx, v1_col] #valeur de v1 pour cette paire (username, password) (= 0 ou 1)
            v2 = df_cred.iloc[indx, v2_col] #valeur de v2 pour cette paire (username, password) (= 0 ou 1
            return v1, v2
        else :#le username et le password ne matchent pas
            abort(make_response(jsonify({'error': 'check username and/or password'}), 403))
    else :#le username et/ou le password ne sont pas référencés ds les credentials
        abort(make_response(jsonify({'error': 'check username and/or password'}), 403))

def perm_resp(username, v1, v2):
    return make_response(jsonify({'username': username, 'v1':bool(v1), 'v2':bool(v2)}), 200)

##API
host="0.0.0.0"
api = Flask(import_name='my_api')

@api.route('/status', methods=['GET']) #renvoie 1 si l’API fonctionne
def api_in_use():
    return  make_response(jsonify({'it works!': '1'}), 200)

@api.route('/permissions/<username>/<password>', methods=['GET']) #renvoie la liste des permissions d’un utilisateur authentifié par son username et son password
def perm_type(username, password):
    v1, v2 = perm_value(username, password)
    perm_json = perm_resp(username, v1, v2)
    return perm_json

@api.route('/v1/sentiment/<username>/<password>/<sentence>', methods=['GET']) #renvoie le score de sentiment v1 de la phrase proposée
def v1_resp(username, password, sentence):
    v1, v2 = perm_value(username, password)
    if v1==1:
        score_v1 = random.randrange(-100, 100, 1)/100
        return  make_response(jsonify({"sentence_sentiment_score": score_v1}), 200)
    elif v1==0:
        abort(make_response(jsonify({'error': 'check username and/or password'}), 403))
    else:
        abort(make_response(jsonify({'error': 'check v1 value'}), 400))

@api.route('/v2/sentiment/<username>/<password>/<sentence>', methods=['GET']) #renvoie le score de sentiment v1 de la phrase proposée
def v2_resp(username, password, sentence):
    v1, v2 = perm_value(username, password)
    if v2==1:
        analyzer = SentimentIntensityAnalyzer()
        sentence = sentence.replace("_", " ").replace("%", " ")
        vs = analyzer.polarity_scores(sentence)
        score_v2 = vs['compound']
        return  make_response(jsonify({"sentence_sentiment_score": score_v2}), 200)
    elif v2==0:
        abort(make_response(jsonify({'error': 'check username and/or password'}), 403))
    else:
        abort(make_response(jsonify({'error': 'check v1 value'}), 400))
    
if __name__ == '__main__':
    api.run(host="0.0.0.0", port=5000)
