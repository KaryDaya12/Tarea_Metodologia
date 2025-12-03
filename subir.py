import csv
from astrapy import DataAPIClient

# ----------- CONFIGURACIÓN ASTRA ------------
TOKEN = "AstraCS:xcSBGezgRkqCmcuSzZHGNJUU:7c32ab13a134a12515c6c461e3f23a3fc52e55658da7d083dfce74c08c017004"

# Tu endpoint REAL
ENDPOINT = "https://a40150f7-e641-4603-9ec5-f80ac9491969-us-east1.apps.astra.datastax.com"
client = DataAPIClient(TOKEN)
db = client.get_database_by_api_endpoint(ENDPOINT)

# Nombre de la colección
collection_name = "instituciones_azuay"

# Crear colección si no existe
if collection_name not in db.list_collection_names():
    db.create_collection(collection_name)

collection = db.get_collection(collection_name)

# ----------- CARGAR CSV ---------------------
csv_file = "instituciones_azuay.csv"   # asegúrate de que esté en la misma carpeta

print("Cargando CSV:", csv_file)

with open(csv_file, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    contador = 0

    for row in reader:
        # Convertimos cada fila del CSV en un documento JSON
        document = dict(row)

        # Insertamos en Astra
        collection.insert_one(document)
        contador += 1

print(f"Documentos guardados correctamente: {contador}")
