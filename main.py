from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import requests
from google.cloud import bigquery
import json
from datetime import datetime
import pandas as pd
import os
import numpy as np

# Configuración
API_LOCAL_URL = os.getenv("API_LOCAL_URL")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")
TOKEN = os.getenv("TOKEN_CR")

def _robust_parse_date(s: pd.Series) -> pd.Series:
    """Intenta parsear fechas en 'YYYY-MM-DD' o 'DD-MM-YYYY'."""
    a = pd.to_datetime(s, errors="coerce", format="%Y-%m-%d")
    b = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return a.fillna(b).dt.date

def normalize_and_filter(df: pd.DataFrame,
                         exclude_codes=None,
                         exclude_texts=None) -> pd.DataFrame:
    """Normaliza fechas y aplica filtros de causales si corresponde."""
    df = df.copy()

    if "fecha_de_ingreso" not in df.columns:
        raise ValueError("Falta columna fecha_de_ingreso")
    if "fecha_finiquito" not in df.columns:
        df["fecha_finiquito"] = np.nan

    df["_f_ingreso"] = _robust_parse_date(df["fecha_de_ingreso"])
    df["_f_finiquito"] = _robust_parse_date(df["fecha_finiquito"])

    today_local = datetime.today().date()
    df["_f_fin_efectivo"] = df["_f_finiquito"].fillna(today_local)

    return df

def build_employee_month_bridge(df: pd.DataFrame) -> pd.DataFrame:
    """Crea tabla empleado×mes con métricas de rotación."""
    df = df.copy()

    min_month = pd.to_datetime(min(df["_f_ingreso"])).to_period("M").to_timestamp()
    max_month = pd.to_datetime(max(df["_f_fin_efectivo"])).to_period("M").to_timestamp()

    months = pd.DataFrame({
        "month_start": pd.date_range(min_month, max_month, freq="MS")
    })
    months["month_end"] = (months["month_start"] + pd.offsets.MonthEnd(0)).dt.date
    months["month_start"] = months["month_start"].dt.date
    months["days_in_month"] = (
        pd.to_datetime(months["month_end"]) - pd.to_datetime(months["month_start"])
    ).dt.days + 1

    df["_key"] = 1
    months["_key"] = 1
    x = df.merge(months, on="_key", how="outer")

    start_ovl = pd.DataFrame({
        "a": pd.to_datetime(x["_f_ingreso"]),
        "b": pd.to_datetime(x["month_start"])
    }).max(axis=1)
    end_ovl = pd.DataFrame({
        "a": pd.to_datetime(x["_f_fin_efectivo"]),
        "b": pd.to_datetime(x["month_end"])
    }).min(axis=1)

    active_days = (end_ovl - start_ovl).dt.days + 1
    x["active_days"] = active_days.clip(lower=0).fillna(0).astype(int)

    x = x[x["active_days"] > 0].copy()

    x["active_on_month_start"] = (
        (pd.to_datetime(x["_f_ingreso"]) <= pd.to_datetime(x["month_start"])) &
        (pd.to_datetime(x["_f_fin_efectivo"]) >= pd.to_datetime(x["month_start"]))
    ).astype(int)

    x["active_on_month_end"] = (
        (pd.to_datetime(x["_f_ingreso"]) <= pd.to_datetime(x["month_end"])) &
        (pd.to_datetime(x["_f_fin_efectivo"]) >= pd.to_datetime(x["month_end"]))
    ).astype(int)

    f_ing_m = pd.to_datetime(x["_f_ingreso"]).dt.to_period("M").dt.to_timestamp()
    f_out_m = pd.to_datetime(x["_f_finiquito"]).dt.to_period("M").dt.to_timestamp()
    month_start_ts = pd.to_datetime(x["month_start"]).dt.to_period("M").dt.to_timestamp()

    x["hire_in_month"] = ((~pd.isna(x["_f_ingreso"])) & (f_ing_m == month_start_ts)).astype(int)
    x["term_in_month"] = ((~pd.isna(x["_f_finiquito"])) & (f_out_m == month_start_ts)).astype(int)

    if "cod_causal_finiquito" in x.columns:
        x["term_causal_code"] = np.where(
            x["term_in_month"].eq(1), x["cod_causal_finiquito"], pd.NA
        )
    else:
        x["term_causal_code"] = pd.NA

    if "causal_finiquito" in x.columns:
        x["term_causal_text"] = np.where(
            x["term_in_month"].eq(1), x["causal_finiquito"], pd.NA
        )
    else:
        x["term_causal_text"] = pd.NA

    x["active_ratio"] = x["active_days"] / x["days_in_month"]
    x["period"] = pd.to_datetime(x["month_start"]).dt.strftime("%Y-%m")

    cols_dims = [c for c in [
        "rut", "nombre_completo", "cliente", "cecos", "cecosorigen",
        "cargo","tipo_empleado", "estado", "instalacion","_f_ingreso","_f_finiquito","_f_fin_efectivo"
    ] if c in x.columns]
    cols_dates = ["month_start", "month_end", "period"]
    cols_metrics = [
        "days_in_month", "active_days", "active_ratio",
        "active_on_month_start", "active_on_month_end",
        "hire_in_month", "term_in_month", "term_causal_code", "term_causal_text"
    ]

    return x[cols_dims + cols_dates + cols_metrics].copy()

def fetch_and_process_data():
    """Función para obtener y procesar datos de la API externa"""
    print("=== OBTENIENDO Y PROCESANDO DATOS ===")
    
    # Preparar parámetros para la API local
    headers = {
        "method": "report",
        "token": TOKEN
    }
    print(f"API URL: {API_LOCAL_URL}")
    print(f"Headers: {headers}")
    
    try:
        print("🔄 Iniciando llamada a ControlRoll...")
        response = requests.get(API_LOCAL_URL, headers=headers, timeout=3600)
        print("✅ Llamada completada")
    except requests.exceptions.Timeout:
        error_msg = "Timeout: La API externa tardó más de 1 hora en responder"
        print(f"❌ {error_msg}")
        raise HTTPException(status_code=504, detail=error_msg)
    except requests.exceptions.ConnectionError as e:
        error_msg = f"Error de conexión con la API externa: {str(e)}"
        print(f"❌ {error_msg}")
        raise HTTPException(status_code=502, detail=error_msg)
    except requests.exceptions.RequestException as e:
        error_msg = f"Error en la petición HTTP: {str(e)}"
        print(f"❌ {error_msg}")
        raise HTTPException(status_code=502, detail=error_msg)
    except Exception as e:
        error_msg = f"Error inesperado: {type(e).__name__}: {str(e)}"
        print(f"❌ {error_msg}")
        import traceback
        print(f"❌ Stack trace: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=error_msg)
    
    print(f"Status code: {response.status_code}")
    response.raise_for_status()
    data_text = response.text
    print(f"Longitud de respuesta: {len(data_text)}")
    print(f"Primeros 200 caracteres: {data_text[:200]}")
    
    data_json = json.loads(data_text)
    print(f"Datos obtenidos: {len(data_json)} registros")
    print(f"Primer registro: {data_json[0] if data_json else 'No hay datos'}")
    
    if not data_json:
        print("No hay datos para procesar")
        return None

    # Convertir a DataFrame
    data = pd.DataFrame(data_json)
    date_columns = ['FECHA DE INGRESO', 'FECHA_FINIQUITO']
    for col in date_columns:
        if col in data.columns:
            data[col] = pd.to_datetime(data[col], format='%d-%m-%Y')
    
    # Normalizar nombres de columnas
    data.columns = data.columns.str.lower()
    data.columns = data.columns.str.replace(' ', '_')
    data.columns = data.columns.str.replace('.', '')
    data.columns = data.columns.str.replace('%', '')
    data.columns = data.columns.str.replace('-', '_')
    data.columns = data.columns.str.replace('(', '')
    data.columns = data.columns.str.replace(')', '')
    data.columns = data.columns.str.replace('á', 'a')
    data.columns = data.columns.str.replace('é', 'e')
    data.columns = data.columns.str.replace('í', 'i')
    data.columns = data.columns.str.replace('ó', 'o')
    data.columns = data.columns.str.replace('ú', 'u')
    data.columns = data.columns.str.replace('ñ', 'n')
    data.columns = data.columns.str.replace('°', '')
    
    # Procesar datos de rotación
    df_norm = normalize_and_filter(data, exclude_codes=[9999], exclude_texts=["Inactivar sin Movimiento"])
    df_norm = df_norm.loc[(df_norm.tipo_empleado!='PART TIME BOLETA')]
    df_bridge = build_employee_month_bridge(df_norm)
    df_bridge = df_bridge[['period','rut', 'cliente', 'instalacion', 'cecos', 'cargo', 'nombre_completo', 'tipo_empleado',
           'estado', '_f_ingreso', '_f_finiquito', 'month_start', 'month_end', 
           'days_in_month', 'active_days', 'active_ratio', 'active_on_month_start',
           'active_on_month_end', 'hire_in_month', 'term_in_month', 'term_causal_text']]

    print(f"✅ Datos procesados exitosamente: {len(df_bridge)} registros")
    return df_bridge

def load_to_bigquery(df_bridge):
    """Función para cargar datos procesados a BigQuery"""
    if df_bridge is None:
        return {
            "success": True,
            "message": "No hay datos para cargar",
            "records_processed": 0
        }
    
    print("=== CARGANDO DATOS A BIGQUERY ===")
    
    try:
        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        )
        
        print(f"🔄 Cargando {len(df_bridge)} registros a BigQuery: {table_id}")
        job = client.load_table_from_dataframe(df_bridge, table_id, job_config=job_config)
        job.result()
        
        print(f"✅ Data cargada exitosamente. {len(df_bridge)} registros cargados a BigQuery.")
        
        return {
            "success": True,
            "message": "Data procesada y cargada exitosamente",
            "records_processed": len(df_bridge)
        }
    except Exception as e:
        error_msg = f"Error al cargar datos en BigQuery: {type(e).__name__}: {str(e)}"
        print(f"❌ {error_msg}")
        import traceback
        print(f"❌ Stack trace: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=error_msg)

def sync_to_bigquery():
    """Función principal para sincronizar datos con BigQuery"""
    print("=== INICIANDO SINCRONIZACIÓN COMPLETA ===")
    
    # Paso 1: Obtener y procesar datos
    df_bridge = fetch_and_process_data()
    
    # Paso 2: Cargar a BigQuery
    result = load_to_bigquery(df_bridge)
    
    return result

# Crear la aplicación FastAPI
app = FastAPI()

@app.get("/")
def root():
    return {"message": "Servicio de sincronización de rotación activo"}

@app.get("/health")
def health_check():
    """Endpoint de salud para verificar el estado del servicio"""
    return {
        "status": "healthy",
        "message": "Servicio funcionando correctamente",
        "timestamp": datetime.now().isoformat()
    }

@app.post("/fetch_data")
def fetch_data():
    """
    Endpoint para obtener y procesar datos de la API externa (sin cargar a BigQuery)
    """
    try:
        df_bridge = fetch_and_process_data()
        if df_bridge is None:
            return {
                "success": True,
                "message": "No hay datos para procesar",
                "records_processed": 0
            }
        
        return {
            "success": True,
            "message": "Datos obtenidos y procesados exitosamente",
            "records_processed": len(df_bridge),
            "columns": list(df_bridge.columns),
            "sample_data": df_bridge.head(3).to_dict('records') if len(df_bridge) > 0 else []
        }
    except Exception as e:
        error_response = {
            "success": False,
            "error": str(e),
            "message": "Error al obtener y procesar datos"
        }
        raise HTTPException(status_code=500, detail=error_response)

@app.post("/load_data")
def load_data():
    """
    Endpoint para cargar datos procesados a BigQuery
    """
    try:
        # Primero obtener los datos
        df_bridge = fetch_and_process_data()
        
        # Luego cargarlos a BigQuery
        result = load_to_bigquery(df_bridge)
        return result
    except Exception as e:
        error_response = {
            "success": False,
            "error": str(e),
            "message": "Error al cargar datos a BigQuery"
        }
        raise HTTPException(status_code=500, detail=error_response)

@app.post("/rotacion_sync")
def rotacion_sync():
    """
    Endpoint para sincronizar datos de rotación (proceso completo)
    """
    try:
        result = sync_to_bigquery()
        return result
    except Exception as e:
        error_response = {
            "success": False,
            "error": str(e),
            "message": "Error al procesar la sincronización"
        }
        raise HTTPException(status_code=500, detail=error_response)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

