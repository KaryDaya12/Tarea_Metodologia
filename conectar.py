from astrapy import DataAPIClient

# Usa tu token REAL aquí
TOKEN = "AstraCS:xcSBGezgRkqCmcuSzZHGNJUU:7c32ab13a134a12515c6c461e3f23a3fc52e55658da7d083dfce74c08c017004"

# Tu endpoint REAL
ENDPOINT = "https://a40150f7-e641-4603-9ec5-f80ac9491969-us-east1.apps.astra.datastax.com"

client = DataAPIClient(TOKEN)
db = client.get_database_by_api_endpoint(ENDPOINT)

print("Conexión exitosa. Colecciones existentes:")
print(db.list_collection_names())

collection = db.create_collection("tramites_azuay")
print("Colección creada:", collection)

