import os, re
import pandas as pd
import streamlit as st
from astrapy import DataAPIClient

# -------------------- CONFIG ASTRA --------------------
TOKEN    = st.secrets.get("TOKEN",    os.getenv("ASTRA_TOKEN",    "AstraCS:xcSBGezgRkqCmcuSzZHGNJUU:7c32ab13a134a12515c6c461e3f23a3fc52e55658da7d083dfce74c08c017004"))
ENDPOINT = st.secrets.get("ENDPOINT", os.getenv("ASTRA_ENDPOINT", "https://a40150f7-e641-4603-9ec5-f80ac9491969-us-east1.apps.astra.datastax.com"))

client = DataAPIClient(TOKEN)
db = client.get_database_by_api_endpoint(ENDPOINT)

# -------------------- UTILIDADES ----------------------
@st.cache_data(ttl=300, show_spinner=False)
def load_collection(name: str) -> pd.DataFrame:
    try:
        col = db.get_collection(name)
        docs = list(col.find({}))
        if not docs:
            return pd.DataFrame()
        df = pd.DataFrame(docs)
        # normalizaciones útiles
        if "_id" in df.columns:   df["_id"] = df["_id"].astype(str)
        return df
    except Exception as e:
        st.warning(f"No pude abrir la colección '{name}': {e}")
        return pd.DataFrame()

def kpi(label, value):
    col = st.columns(1)[0]
    col.metric(label, f"{value:,}")

def top_words(series: pd.Series, k=15):
    # tokenizador simple
    txt = " ".join([str(x) for x in series.dropna().tolist()])
    tokens = [w.lower() for w in re.findall(r"[a-záéíóúñüA-ZÁÉÍÓÚÑÜ]{3,}", txt)]
    stop = set("para por con los las una uno del de y en que como desde sobre según según".split())
    tokens = [t for t in tokens if t not in stop]
    return pd.Series(tokens).value_counts().head(k)

# -------------------- LAYOUT --------------------------
st.set_page_config(page_title="CRISP-DM + AstraDB", page_icon="📊", layout="wide")
st.title("📊 Proyecto CRISP-DM con AstraDB (DataAPI) + Streamlit")

st.sidebar.header("Navegación")
page = st.sidebar.radio(
    "Fases",
    ["1. Entendimiento del negocio",
     "2. Entendimiento de los datos",
     "3. Preparación",
     "4. Modelado",
     "5. Evaluación / Dashboard"]
)

# Colecciones disponibles
collections = db.list_collection_names()
st.sidebar.caption("Colecciones en Astra")
st.sidebar.write(collections)

# ======================================================
# 1) ENTENDIMIENTO DEL NEGOCIO
# ======================================================
if page.startswith("1."):
    st.subheader("🎯 Objetivo")
    st.write(
        "- Integrar una **base NoSQL** (AstraDB DataAPI) con visualización interactiva.\n"
        "- Dataset: **Instituciones y Trámites** (Azuay / Ecuador).\n"
        "- Entregables: app en Streamlit, repo, y narrativa CRISP-DM."
    )
    st.subheader("👩🏽‍💻 Arquitectura")
    st.markdown("""
    - **Ingesta**: CSV / API → AstraDB (colecciones JSON)
    - **Almacenamiento**: `default_keyspace` en AstraDB
    - **Exploración**: Streamlit + pandas
    - **KPIs**: conteos, top instituciones, palabras clave
    """)
    st.info(f"Conectado a: `{ENDPOINT}`")

# ======================================================
# 2) ENTENDIMIENTO DE LOS DATOS
# ======================================================
elif page.startswith("2."):
    st.subheader("🔎 Entendimiento de los datos")
    col_name = st.selectbox("Selecciona la colección a explorar", options=collections or ["instituciones_azuay"], index=0)
    df = load_collection(col_name)

    if df.empty:
        st.warning("No hay documentos en esta colección.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("📄 Documentos", len(df))
        c2.metric("🔢 Columnas", len(df.columns))
        c3.metric("🧪 Nulos totales", int(df.isna().sum().sum()))
        st.dataframe(df, use_container_width=True, height=420)

        # Perfilado mínimo
        st.subheader("📌 Campos más poblados")
        st.bar_chart((~df.isna()).sum().sort_values(ascending=False))

        if "nombre" in df.columns:
            st.subheader("🔠 Distribución por inicial")
            tmp = df["nombre"].dropna().str.strip().str[0].value_counts().sort_values(ascending=False).head(15)
            st.bar_chart(tmp)

# ======================================================
# 3) PREPARACIÓN
# ======================================================
elif page.startswith("3."):
    st.subheader("🧹 Preparación de datos")
    col_name = st.selectbox("Colección origen", options=collections or ["instituciones_azuay"], index=0, key="prep")
    df = load_collection(col_name)

    if df.empty:
        st.warning("No hay datos para preparar.")
    else:
        # Ejemplos de limpieza mínima
        st.write("**Limpiezas aplicadas**:")
        st.write("- Trim a texto en `nombre` (si existe)\n- Eliminación de duplicados por `_id`")
        if "nombre" in df.columns:
            df["nombre"] = df["nombre"].astype(str).str.strip()

        if "_id" in df.columns:
            df = df.drop_duplicates(subset=["_id"])

        st.dataframe(df.head(50), use_container_width=True, height=360)
        st.success("Datos listos en memoria (no se reescriben en Astra).")

        # Palabras clave (si hay campo de texto)
        text_col = st.selectbox("Campo de texto para palabras clave", options=[c for c in df.columns if df[c].dtype=='object'])
        if text_col:
            st.subheader(f"🔎 Palabras clave más frecuentes en `{text_col}`")
            st.bar_chart(top_words(df[text_col], 20))

# ======================================================
# 4) MODELADO (simple, descriptivo)
# ======================================================
elif page.startswith("4."):
    st.subheader("🧠 Modelado (descriptivo)")
    st.write("Para este entregable, incluimos **modelado descriptivo**: rankings y segmentaciones simples.")
    src = st.selectbox("Colección", options=collections or ["instituciones_azuay"], index=0, key="model")
    df = load_collection(src)

    if df.empty:
        st.warning("No hay datos para modelar.")
    else:
        if "nombre" in df.columns:
            df["largo_nombre"] = df["nombre"].astype(str).apply(len)
            st.write("**Top 10 instituciones por longitud del nombre**")
            top10 = df.sort_values("largo_nombre", ascending=False).head(10)
            st.dataframe(top10[["nombre", "largo_nombre"]], use_container_width=True)
            st.bar_chart(top10.set_index("nombre")["largo_nombre"])

        # Si existen categorías/canales en 'tramites_azuay'
        if "categorias" in df.columns:
            st.write("**Top categorías (si existen en tramites)**")
            exploded = df["categorias"].explode().dropna().astype(str)
            st.bar_chart(exploded.value_counts().head(15))

# ======================================================
# 5) EVALUACIÓN / DASHBOARD
# ======================================================
elif page.startswith("5."):
    st.subheader("📈 Dashboard (KPIs)")
    colec = st.selectbox("Colección", options=collections or ["instituciones_azuay"], index=0, key="eval")
    df = load_collection(colec)

    if df.empty:
        st.warning("No hay datos.")
    else:
        c1, c2 = st.columns(2)
        c1.metric("Documentos", len(df))
        c2.metric("Columnas", len(df.columns))

        # Buscador
        st.text_input("🔎 Buscar por nombre", key="q")
        q = st.session_state.get("q", "").strip()
        if q and "nombre" in df.columns:
            view = df[df["nombre"].str.contains(q, case=False, na=False)]
        else:
            view = df

        st.dataframe(view.head(200), use_container_width=True, height=360)

        # Gráfico rápido
        if "nombre" in df.columns:
            st.subheader("🔠 Inicial del nombre")
            initial = df["nombre"].dropna().str.strip().str[0].value_counts().head(20)
            st.bar_chart(initial)
