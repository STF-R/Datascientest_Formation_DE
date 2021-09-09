import os
import requests

# définition de l'adresse de l'API
api_address = 'fastapi'

# port de l'API
api_port = 8000

# requête
usernames = 'alice', 'bob', 'clementine'
passwords = 'wonderland', 'builder', 'mandarine'
expected_results = 200, 200, 403
for user, passwd, expected_result in zip(usernames, passwords, expected_results):
    r = requests.get(
        url='http//{address}:{port}/permissions'.format(address=api_address, port=api_port),
        params= {
            'username': user,
            'password': passwd
        }
    )

    output = '''
    ============================
        Authentication test
    ============================

    request done at "/permissions"
    | username = {user}
    | password = {passwd}
    expected result = {expected_result}
    actual restult = {status_code}

    ==>  {test_status}

    '''

    # statut de la requête
    status_code = r.status_code

    # affichage des résultats
    if status_code == expected_result:
        test_status = 'SUCCESS'
    else:
        test_status = 'FAILURE'
    print(output.format(user=user, passwd=passwd, expected_result=expected_result, status_code=status_code, test_status=test_status))

    # impression dans un fichier
    if os.environ.get('LOG') == 1:
        with open('api_test.log', 'a') as file:
            file.write(output)
