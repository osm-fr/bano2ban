import requests
import psycopg2
import json
import re
import secret # contient les identifiants de connexion à l'API

api = 'https://api-ban.ign.fr'
auth_token = None

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
conn = psycopg2.connect("dbname=cquest user=cquest")
cur = conn.cursor()
cur2 = conn.cursor()

# on vérifie la liste des voies
cur.execute("SELECT fantoir, voie_cadastre FROM cumul_adresses WHERE source = 'OD-MONTPELLIER' group by 1,2;")
groups = cur.fetchall()
print(len(groups),' groupes dans BANO')
for group in groups:
    get = requests.get(api+'/group/fantoir:'+group[0][:9], headers={'Authorization':'Bearer '+auth_token})
    if get.status_code == 200: # on a trouvé la voie
        ban_group = json.loads(get.text)
        name = re.sub('  ',' ',group[1]) # nettoyage des espaces multiples...
        print(group[0][:9], name)
        if name != ban_group['name']: # nom différent
            print('  Nom différent: %s <-> %s' % (name, ban_group['name']))
            if ban_group['name'] ==  ban_group['name'].upper() or len(name) != len(ban_group['name']):
                patch = requests.patch(api+'/group/fantoir:'+group[0][:9], headers={'Authorization':'Bearer '+auth_token}, json={'name': name, 'version': ban_group['version']+1})
                print('  Mise à jour', patch.status_code)

        # contrôle des housenumbers de la voie
        cur2.execute("SELECT numero, st_x(geometrie) as lon, st_y(geometrie) as lat FROM cumul_adresses WHERE source = 'OD-MONTPELLIER' and fantoir=%s order by 1;", (group[0],))
        housenumbers = cur2.fetchall()

        # récupération depuis la BAN
        get = requests.get(api+'/housenumber?limit=1000&group=fantoir:'+group[0][:9], headers={'Authorization':'Bearer '+auth_token})
        ban_housenumbers = json.loads(get.text)

        for housenumber in housenumbers:
            od_number = re.sub('[^0-9]', '', housenumber[0])
            od_ordinal = re.sub(od_number, '', housenumber[0]).strip().lower()
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
                        print("    "+h['number']+h['ordinal']+" > "+housenumber[0])
                        payload = {'ordinal': od_ordinal, 'version': h['version']+1}
                        patch = requests.patch(api+'/housenumber/'+h['id'], headers={'Authorization':'Bearer '+auth_token}, json=payload)
                        print(patch.text)
                        ban_num = h
                        break
            if ban_num is None: # numéro non trouvé
                print("    N° " + housenumber[0] + " non trouvé")
                # on ajoute le housenumber manquant
                payload = {'number': od_number, 'ordinal': od_ordinal, 'parent': ban_group['id'], 'attributes': {'created_from': 'Opendata Montpellier'} }
                post = requests.post(api+'/housenumber', headers={'Authorization':'Bearer '+auth_token}, json=payload)
                print(post.text)

    else:
        print('Fantoir %s non trouvé' % (group[0], ))