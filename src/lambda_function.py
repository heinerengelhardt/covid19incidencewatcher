import requests, json, boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key

def lambda_handler(event, context):
    # Initialisierung
    lk    = event['lk']       # Landkreis Bezeichnung aus Event holen
    lk_id = event['lk_id']    # Landkreis ID aus Event holen
    bl    = event['bl']       # Bundesland Bezeichnung aus Event holen

    # Persistierten Stand der Inzidenz in DynamoDB abfragen
    getdatasetresult = get_corona_dataset(lk)    # Datensatz aus DynamoDB lesen
    oldincidence = None
    if getdatasetresult:
        oldincidence = getdatasetresult['Inz7T']    # Persistierte Inzidenzt extrahieren
    
    # Online Stand per Rest API abfragen, anreichern, persistieren und Inzidenz extrahieren
    url = 'https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/rki_key_data_v/FeatureServer/0/query?'
    parameter = {
        'referer':'https://www.amazon.com',
        'user-agent':'python-requests/3.7',
        'where': f'AdmUnitId = {lk_id}',      # Welche Landkreise sollen zurück gegeben werden
        'outFields': '*',                     # Rückgabe aller Felder
        'returnGeometry': False,              # Keine Geometrien
        'f':'json',                           # Rückgabeformat, hier JSON
        'cacheHint': True                     # Zugriff über CDN
    }
    response          = requests.get(url=url, params=parameter)           # Anfrage absetzen
    responsejson      = json.loads(response.text, parse_float=Decimal)    # Das Ergebnis JSON als Python Dictionary laden
    responsejsonclean = responsejson['features'][0]['attributes']         # Wir erwarten genau einen Datensatz, Ausgabe aller Attribute
    responsejsonclean['lk'] = lk;                                         # Ergebnis anreichern um Landkreis Bezeichnung
    responsejsonclean['bl'] = bl;                                         # Ergebnis anreichern um Bundesland Bezeichnung
    putdatasetresult = put_corona_dataset(responsejsonclean)              # Ergebnis in DynamoDB persistieren
    newincidence = responsejsonclean['Inz7T']                             # Online abgefragte Inzidenz extrahieren
    
    # Wenn persistierte Inzidenz gleich abgefragter Inzidenz dann nicht notifizieren sonst schon
    if oldincidence == newincidence:
        raise Exception('no incidence change')
    else:
        return 'NEUE INZIDENZ IN ' + str(bl) + ' IM LANDKREIS ' + str(lk) + '!   ALT: ' + str(oldincidence) + '   NEU: ' + str(newincidence)

def put_corona_dataset(item):
    dynamodb = boto3.resource('dynamodb')
    table    = dynamodb.Table('corona')
    response = table.put_item(Item=item)
    return response

def get_corona_dataset(lk):
    dynamodb = boto3.resource('dynamodb')
    table    = dynamodb.Table('corona')
    response = table.query(
        KeyConditionExpression=Key('lk').eq(lk)
    )
    items = response['Items']
    if items:
        return items[0]
    else:
        return []
