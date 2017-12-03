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
    auth = requests.post(api+'/token/', json=payload)
    token = json.loads(auth.text)
    return token['access_token']


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
    get = requests.get(api+'/group/fantoir:'+group['fantoir'], headers={'Authorization':'Bearer '+auth_token})
    if get.status_code == 200: # on a trouvé la voie
        ban_group = json.loads(get.text)
        name = re.sub('  ',' ',group[1]) # nettoyage des espaces multiples...
        print(group['fantoir'], name)
        if name != ban_group['name']: # nom différent
            print('  Nom différent: %s <-> %s' % (name, ban_group['name']))
            if ban_group['name'] ==  ban_group['name'].upper() or len(name) != len(ban_group['name']):
                patch = requests.patch(api+'/group/fantoir:'+group['fantoir'], headers={'Authorization':'Bearer '+auth_token}, json={'name': name, 'version': ban_group['version']+1})
                print('  Mise à jour', patch.status_code)

        # contrôle des housenumbers de la voie
        cur2.execute("SELECT numero, st_x(geometrie) as lon, st_y(geometrie) as lat, source FROM cumul_adresses WHERE insee_com=%s AND source ~ '^OD-' and fantoir~%s order by 1;", (insee, '^'+group['fantoir']))
        housenumbers = cur2.fetchall()

        # récupération depuis la BAN
        get = requests.get(api+'/housenumber?limit=1000&group=fantoir:'+group['fantoir'], headers={'Authorization':'Bearer '+auth_token})
        ban_housenumbers = json.loads(get.text)

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
                        patch = requests.patch(api+'/housenumber/'+h['id'], headers={'Authorization':'Bearer '+auth_token}, json=payload)
                        print('    mise à jour housenumber/ordinal', patch.status_code)
                        ban_num = json.loads(patch.text)
                        break
            if ban_num is None: # numéro non trouvé
                print("    N° " + housenumber['numero'] + " non trouvé")
                # on ajoute le housenumber manquant
                payload = {'number': od_number, 'ordinal': od_ordinal, 'parent': ban_group['id'], 'attributes': {'created_from': 'Opendata Montpellier'} }
                post = requests.post(api+'/housenumber', headers={'Authorization':'Bearer '+auth_token}, json=payload)
                ban_num = json.loads(post.text)
                print('    ajout housenumber > '+ban_num['id'])
                payload = {
                    'housenumber': ban_num['id'],
                    'attributes': {'comment': 'Opendata Montpellier'},
                    'kind':'entrance',
                    'positioning':'gps',
                    'center':{'coordinates': [housenumber['lon'],housenumber['lat']]}
                }
                post = requests.post(api+'/position', headers={'Authorization':'Bearer '+auth_token}, json=payload)
                ban_pos = json.loads(post.text)
                print('      ajout position N° %s > %s' % (housenumber['numero'], ban_pos['id']))
            else:
                # maintenant les positions...
                get = requests.get(api+'/position?housenumber='+ban_num['id'], headers={'Authorization':'Bearer '+auth_token})
                ban_positions = json.loads(get.text)
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
                    post = requests.post(api+'/position', headers={'Authorization':'Bearer '+auth_token}, json=payload)
                    ban_pos = json.loads(post.text)
                    print('      ajout position N° %s > %s' % (housenumber['numero'], ban_pos['id']))

    else:
        print('Fantoir %s non trouvé' % (group['fantoir'], ))
