import requests
from astrapy import DataAPIClient

# ------------------ Configuración ------------------
TOKEN = "AstraCS:xcSBGezgRkqCmcuSzZHGNJUU:7c32ab13a134a12515c6c461e3f23a3fc52e55658da7d083dfce74c08c017004"
ENDPOINT = "https://a40150f7-e641-4603-9ec5-f80ac9491969-us-east1.apps.astra.datastax.com"


client = DataAPIClient(TOKEN)
db = client.get_database_by_api_endpoint(ENDPOINT)

# Crear colección si no existe
collection_name = "tramites_page0"
if collection_name not in db.list_collection_names():
    db.create_collection(collection_name)

collection = db.get_collection(collection_name)

BASE = "https://www.gob.ec/api/v1"

# ---------------- EXTRAER PÁGINA 0 ----------------
print("Consultando trámites de la página 0...")

url = f"{BASE}/tramites?page=0"
response = requests.get(url, timeout=20)

print("Status:", response.status_code)
print("Primeros 300 caracteres de la respuesta:")
print(response.text[:300])

if response.status_code != 200:
    print("La API no devolvió datos.")
    exit()

try:
    data = response.json()
except Exception as e:
    print("Error convirtiendo JSON:", e)
    exit()

print(f"Cantidad de trámites recibidos: {len(data)}")

# ---------------- GUARDAR EN ASTRA ----------------
contador = 0
for tramite in data:
    try:
        collection.insert_one(tramite)
        contador += 1
    except Exception as e:
        print("Error guardando un trámite:", e)

print(f"\nIngesta completada. Total guardados: {contador}")
