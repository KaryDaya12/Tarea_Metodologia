import requests
import time
import csv
from datetime import datetime

BASE = "https://www.gob.ec/api/v1"

# ---------------------------------------------------
#  SESIÓN HTTP REUTILIZABLE (mejor rendimiento)
# ---------------------------------------------------
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    })
    return s

S = make_session()

def get_json(url, timeout=20):
    r = S.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------
#  CONFIG: Provincias permitidas
# ---------------------------------------------------
PROVINCIAS_FILTRO = {"azuay"}


def es_provincia_permitida(texto):
    if not texto:
        return False
    return texto.lower() in PROVINCIAS_FILTRO


def es_2024(fecha_str):
    """Verifica si updated_at pertenece al año 2024"""
    if not fecha_str:
        return False
    try:
        fecha = datetime.fromisoformat(fecha_str.replace("Z", ""))
        return fecha.year == 2024
    except:
        return False


# ---------------------------------------------------
#  LISTAR INSTITUCIONES SOLO DE PROVINCIAS FILTRADAS
# ---------------------------------------------------
def listar_instituciones_filtradas(max_pages=150, sleep_s=0.1):
    instituciones = []

    for p in range(max_pages):
        data = get_json(f"{BASE}/instituciones?page={p}")
        items = data.get("results", data)

        if not items:
            break

        for inst in items:
            prov = inst.get("provincia")
            if es_provincia_permitida(prov):
                instituciones.append({
                    "institucion_id": inst.get("institucion_id") or inst.get("id"),
                    "nombre": inst.get("nombre"),
                    "provincia": prov,
                    "canton": inst.get("canton"),
                    "parroquia": inst.get("parroquia"),
                    "direccion": inst.get("direccion"),
                    "telefono": inst.get("telefono"),
                    "email": inst.get("email"),
                    "categoria": inst.get("categoria"),
                    "website": inst.get("website"),
                    "horario_atencion": inst.get("horario_atencion")
                })

        time.sleep(sleep_s)

    return instituciones


# ---------------------------------------------------
#  DETALLE DEL TRÁMITE
# ---------------------------------------------------
def obtener_tramite(tramite_id):
    try:
        det = get_json(f"{BASE}/tramites/{tramite_id}")
        return {
            "tramite_id": tramite_id,
            "nombre": det.get("nombre"),
            "descripcion": det.get("descripcion"),
            "costo": det.get("costo"),
            "tiempo_respuesta": det.get("tiempo_respuesta"),
            "categoria": det.get("categoria"),
            "estado": det.get("estado"),
            "updated_at": det.get("updated_at"),
            "institucion_id": det.get("institucion", {}).get("id")
        }
    except:
        return {}


# ---------------------------------------------------
#  LISTAR TRÁMITES DE UNA INSTITUCIÓN (solo 2024)
# ---------------------------------------------------
def listar_tramites_filtrados(inst_id, max_pages=40, sleep_s=0.05):
    tramites = []

    for p in range(max_pages):
        data = get_json(f"{BASE}/tramites?institution={inst_id}&page={p}")
        items = data.get("results", data)

        if not items:
            break

        for t in items:
            tid = t.get("id") or t.get("tramite_id")
            detalle = obtener_tramite(str(tid))

            # Filtrar por updated_at del 2024
            if detalle and es_2024(detalle.get("updated_at")):
                tramites.append(detalle)

        time.sleep(sleep_s)

    return tramites


# ---------------------------------------------------
#  EXPORTAR CSV
# ---------------------------------------------------
def exportar_csv(nombre, lista):
    if not lista:
        print(f"No hay datos para {nombre}")
        return

    with open(nombre, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=lista[0].keys())
        writer.writeheader()
        writer.writerows(lista)

    print(f"✔ CSV generado: {nombre}")


# ---------------------------------------------------
#  MAIN
# ---------------------------------------------------
if __name__ == "__main__":
    print("Descargando instituciones de Azuay y Pichincha...")
    instituciones = listar_instituciones_filtradas()
    exportar_csv("instituciones_filtradas.csv", instituciones)

    print("Descargando trámites 2024...")
    todos = []

    for inst in instituciones:
        inst_id = inst["institucion_id"]
        tramites = listar_tramites_filtrados(inst_id)
        todos.extend(tramites)

    exportar_csv("tramites_filtrados_2024.csv", todos)

    print("✔ Datos listos para el tablero")

