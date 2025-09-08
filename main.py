import functions_framework
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
        "cargo", "estado", "instalacion","_f_ingreso","_f_finiquito","_f_fin_efectivo"
    ] if c in x.columns]
    cols_dates = ["month_start", "month_end", "period"]
    cols_metrics = [
        "days_in_month", "active_days", "active_ratio",
        "active_on_month_start", "active_on_month_end",
        "hire_in_month", "term_in_month", "term_causal_code", "term_causal_text"
    ]

    return x[cols_dims + cols_dates + cols_metrics].copy()

def sync_to_bigquery():
    """Función principal para sincronizar datos con BigQuery"""
    # Inicializar cliente de BigQuery
    client = bigquery.Client(project=PROJECT_ID)
    print("=== INICIANDO SINCRONIZACIÓN ===")
    
    # Preparar parámetros para la API local
    headers = {
        "method": "report",
        "token": TOKEN
    }
    print(API_LOCAL_URL)
    print(PROJECT_ID)
    print(DATASET_ID)
    print(TABLE_ID)
    print(TOKEN)
    print(f"Llamando a API local: {API_LOCAL_URL}")
    print(f"Headers: {headers}")
    
    try:
        print("🔄 Iniciando llamada a ControlRoll...")
        response = requests.get(API_LOCAL_URL, headers=headers, timeout=3600)
        print("✅ Llamada completada")
    except Exception as e:
        print(f"❌ ERROR CRÍTICO: {type(e).__name__}: {e}")
        print(f"❌ Detalles del error: {str(e)}")
        import traceback
        print(f"❌ Stack trace: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Error crítico: {e}")
    
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
        return {
            "status": "success",
            "message": "No hay datos para cargar",
            "rows_inserted": 0
        }

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
    df_bridge = df_bridge[['period','rut', 'cliente', 'instalacion', 'cecos', 'cargo', 'nombre_completo', 'cargo','tipo_empleado'
           'estado', '_f_ingreso', '_f_finiquito', 'month_start', 'month_end', 
           'days_in_month', 'active_days', 'active_ratio', 'active_on_month_start',
           'active_on_month_end', 'hire_in_month', 'term_in_month', 'term_causal_text']]

    # Cargar a BigQuery
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE"
    )
    
    job = client.load_table_from_dataframe(df_bridge, table_id, job_config=job_config)
    job.result()
    
    print(f"Data procesada exitosamente. {len(df_bridge)} registros cargados a BigQuery.")
    
    return {
        "success": True,
        "message": "Data procesada exitosamente",
        "records_processed": len(df_bridge)
    }

@functions_framework.http
def rotacion_sync(request):
    """
    Cloud Function HTTP endpoint para sincronizar datos de rotación
    """
    # Configurar CORS
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    # Configurar headers de respuesta
    headers = {
        'Access-Control-Allow-Origin': '*'
    }

    try:
        result = sync_to_bigquery()
        return (JSONResponse(content=result).body.decode(), 200, headers)
    except Exception as e:
        error_response = {
            "success": False,
            "error": str(e),
            "message": "Error al procesar la sincronización"
        }
        return (JSONResponse(content=error_response).body.decode(), 500, headers)
