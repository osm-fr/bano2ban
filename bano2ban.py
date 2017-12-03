import requests
import psycopg2
import psycopg2.extras
import json
import re
import sys
import secret # contient les identifiants de connexion à l'API

api = 'https://api-ban.ign.fr'
auth_token = None
if len(sys.argv)>1:
    insee = sys.argv[1]
else:
    insee = '34172' # Montpellier

def getAuthToken():
    payload={
        'grant_type': 'client_credentials',
        'client_id': secret.id,
        'client_secret': secret.secret,
        'email': 'bano@openstreetmap.fr'
        }
    (status,token) = call_api('POST','/token/', payload)
    return token['access_token']

def call_api(method, endpoint, payload=None):
    # header si on est authentifié
    if auth_token is not None:
        headers={'Authorization':'Bearer '+auth_token}
    else:
        headers={}
    if method == 'GET':
        result = requests.get(api+endpoint, headers=headers)
    elif method == 'POST':
        result = requests.post(api+endpoint, headers=headers, json=payload)
    elif method == 'PATCH':
        result = requests.patch(api+endpoint, headers=headers, json=payload)
    elif method == 'DELETE':
        result = requests.delete(api+endpoint, headers=headers, json=payload)
    return(result.status_code, json.loads(result.text))

# on récupère le token d'authentification pour les appels suivants
auth_token = getAuthToken()
conn = psycopg2.connect("dbname=cquest user=cquest", cursor_factory=psycopg2.extras.DictCursor)
cur = conn.cursor()
cur2 = conn.cursor()

# on vérifie la liste des voies
cur.execute("SELECT left(fantoir,9) as fantoir, voie_cadastre FROM cumul_adresses WHERE insee_com=%s AND source ~ '^OD-' group by 1,2;" , (insee,))
groups = cur.fetchall()
print(len(groups),' groupes dans BANO')
for group in groups:
    (status, ban_group) = call_api('GET', '/group/fantoir:'+group['fantoir'])
    if status == 200: # on a trouvé la voie
        name = re.sub('  ',' ',group[1]) # nettoyage des espaces multiples...
        print(group['fantoir'], name)
        if name != ban_group['name']: # nom différent
            print('  Nom différent: %s <-> %s' % (name, ban_group['name']))
            if ban_group['name'] ==  ban_group['name'].upper() or len(name) != len(ban_group['name']):
                status = call_api('PATCH', '/group/fantoir:'+group['fantoir'], payload={'name': name, 'version': ban_group['version']+1})
                print('  Mise à jour', status)

        # contrôle des housenumbers de la voie
        cur2.execute("SELECT numero, st_x(geometrie) as lon, st_y(geometrie) as lat, source FROM cumul_adresses WHERE insee_com=%s AND source ~ '^OD-' and fantoir~%s order by 1;", (insee, '^'+group['fantoir']))
        housenumbers = cur2.fetchall()

        # récupération depuis la BAN
        (status,ban_housenumbers) = call_api('GET','/housenumber?limit=1000&group=fantoir:'+group['fantoir'])

        # vérification des numéros dans BAN et absents dans source
        for h in ban_housenumbers['collection']:
            num = None
            if h['number'] is None:
                continue
            if h['ordinal'] is None:
                h['ordinal'] = ''
            for housenumber in housenumbers:
                od_number = re.sub('[^0-9]', '', housenumber['numero'])
                od_ordinal = re.sub(od_number, '', housenumber['numero']).strip().lower()
                if h['number'] == od_number and h['ordinal'].upper() == od_ordinal.upper():
                    num = housenumber
                    break
            if num is None and h['ordinal'] != '':
                for housenumber in housenumbers:
                    od_number = re.sub('[^0-9]', '', housenumber['numero'])
                    od_ordinal = re.sub(od_number, '', housenumber['numero']).strip().lower()
                    if h['number'] == od_number and od_ordinal !='' and h['ordinal'][0].upper() == od_ordinal[0].upper():
                        num = housenumber
                        break
            if num is None:
                print('    N°%s%s (BAN) absent de la source' % (h['number'],h['ordinal']))
                if 'comment' not in h:
                    status = call_api('PATCH', '/housenumber/'+h['id'], {'comment': 'absent de Opendata Montpellier', 'version': h['version']+1})

        for housenumber in housenumbers:
            od_number = re.sub('[^0-9]', '', housenumber['numero'])
            od_ordinal = re.sub(od_number, '', housenumber['numero']).strip().lower()
            ban_num = None
            for h in ban_housenumbers['collection']:
                if h['ordinal'] is None:
                    h['ordinal']=''
                if h['number'] == od_number and h['ordinal'].upper() == od_ordinal.upper():
                    # numéro et ordinal identiques
                    ban_num = h
                    break
            if ban_num is None:
                # on cherche avec ordinal tronqué...
                for h in ban_housenumbers['collection']:
                    if h['ordinal'] is None:
                        h['ordinal']=''
                    if h['number'] == od_number and h['ordinal']!='' and od_ordinal !='' and h['ordinal'][0].upper() == od_ordinal[0].upper():
                        # numéro identique mais ordinal abrégé ?
                        print("    "+h['number']+h['ordinal']+" > "+housenumber['numero'])
                        payload = {'ordinal': od_ordinal, 'version': h['version']+1}
                        (status, ban_num) = call_api('PATCH','/housenumber/'+h['id'], payload)
                        print('    mise à jour housenumber/ordinal', status)
                        break
            if ban_num is None: # numéro non trouvé
                print("    N° " + housenumber['numero'] + " non trouvé")
                # on ajoute le housenumber manquant
                payload = {'number': od_number, 'ordinal': od_ordinal, 'parent': ban_group['id'], 'attributes': {'created_from': 'Opendata Montpellier'} }
                (status, ban_num) = call_api('POST','/housenumber', payload)
                print('    ajout housenumber > '+ban_num['id'])
                payload = {
                    'housenumber': ban_num['id'],
                    'attributes': {'comment': 'Opendata Montpellier'},
                    'kind':'entrance',
                    'positioning':'gps',
                    'center':{'coordinates': [housenumber['lon'],housenumber['lat']]}
                }
                (status, ban_pos) = call_api('POST', '/position', payload)
                print('      ajout position N° %s > %s' % (housenumber['numero'], ban_pos['id']))
            else:
                # maintenant les positions...
                (status,ban_positions) = call_api('GET','/position?housenumber='+ban_num['id'])
                ban_pos = None
                for pos in ban_positions['collection']:
                    if pos['center']['coordinates'][0] == housenumber['lon'] and pos['center']['coordinates'][1] == housenumber['lat']:
                        ban_pos = pos
                        break
                if ban_pos is None: # position pas trouvée, on l'ajoute
                    payload = {
                        'housenumber': ban_num['id'],
                        'attributes': {'comment': 'Opendata Montpellier'},
                        'kind':'entrance',
                        'positioning':'gps',
                        'center':{'coordinates': [housenumber['lon'],housenumber['lat']]}
                    }
                    (status, ban_pos) = call_api('POST','/position', payload)
                    print('      ajout position N° %s > %s' % (housenumber['numero'], ban_pos['id']))

    else:
        print('Fantoir %s non trouvé' % (group['fantoir'], ))
