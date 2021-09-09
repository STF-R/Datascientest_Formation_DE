import json
import os
import requests

# définition de l'adresse de l'API
api_address = 'fastapi'

# port de l'API
api_port = 8000

# requête
username = 'alice'
password = 'wonderland'
sentences = "life is beautiful", "that sucks"
endpoint = '/v2/sentiment'

for i, sentence in enumerate(sentences):
    r = requests.get(
        url='http//{address}:{port}{end}'.format(address=api_address, port=api_port, end=endpoint),
        params= {
            'username': user,
            'password': passwd,
            'sentence': sentence
        }
    )
    response = r.json()
    score = float(response['score'])
    
    if i==0:
        expected_result = "superieur a 0"
        if score > 0:
            test_status = 'SUCCESS'
        else:
            test_status = 'FAILURE'
    elif i==1:
        expected_result = "inferieur a 0"
        if score < 0:
            test_status = 'SUCCESS'
        else:
            test_status = 'FAILURE' 

    output = '''
    ============================
        Authentication test
    ============================

    request done at {endpoint}
    | username = {user}
    | password = {passwd}
    expected result = {expected_result}
    actual result = {score}

    ==>  {test_status}

    '''

    print(output.format(endpoint=endpoint, user=user, passwd=passwd, expected_result=expected_result, score=score, test_status=test_status))

    # impression dans un fichier
    if os.environ.get('LOG') == 1:
        with open('api_test.log', 'a') as file:
            file.write(output)
