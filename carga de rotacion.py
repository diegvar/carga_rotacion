# -*- coding: utf-8 -*-
"""
Created on Thu Sep  4 13:09:44 2025

@author: Diego
"""

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json


# CONFIGURACIÓN - CAMBIA ESTOS VALORES
# 1. Ruta al archivo JSON de la cuenta de servicio
SERVICE_ACCOUNT_KEY_FILE = r'C:/Worldwide/GCP/pruebas-463316-11f299ec9260.json'
# 2. ID del proyecto de Google Cloud
PROJECT_ID = "pruebas-463316"
# 3. ID del dataset en BigQuery
DATASET_ID = "ww_data_upload_asistencia"
# 4. Nombre de la tabla en BigQuery
TABLE_ID = "testdata"


data = pd.read_excel(r'C:\Worldwide\Libro Haberes y Descuentos 202507.xlsx')
data.rename(columns={'AFC':'AFC_'},inplace=True)
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





data['egreso']=0
data['ingreso']=0
data.loc[data['fecha_de_finiquitohistorial'].dt.strftime('%Y-%m').str.replace('-', '')==data['periodo'].astype(str),'egreso']=1
data.loc[data['fecha_de_ingreso'].dt.strftime('%Y-%m').str.replace('-', '')==data['periodo'].astype(str),'ingreso']=1

data=data[['rut_dv','fecha_de_ingreso','fecha_de_finiquitohistorial','fecha_de_finiquitohoy','descripcion_causal','cc','cliente','sector','instalacion','cargo','jornada','tipo_contrato','total_imponible_hora_extra','egreso','ingreso']]

data=data.reset_index(drop=True)


credentials = service_account.Credentials.from_service_account_file(
   SERVICE_ACCOUNT_KEY_FILE
)
client = bigquery.Client(credentials=credentials, project="pruebas-463316")
table_id = "pruebas-463316.ww_data_upload_asistencia.testdata"

job_config = bigquery.LoadJobConfig(

    write_disposition="WRITE_TRUNCATE",  # or WRITE_APPEND, WRITE_EMPTY
)
job = client.load_table_from_dataframe(data, table_id, job_config=job_config)

# Wait for the job to complete
job.result()


