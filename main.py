import pandas as pd
from google.cloud import bigquery, storage
import functions_framework
import io
import os

# Configuración
PROJECT_ID = "pruebas-463316"
DATASET_ID = "ww_data_upload_asistencia"
TABLE_ID = "testdata"

@functions_framework.cloud_event
def process_rotacion_file(cloud_event):
    """
    Cloud Function que se ejecuta cuando se sube un archivo a Storage
    """
    # Obtener información del archivo subido
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]
    
    print(f"Procesando archivo: {file_name} del bucket: {bucket_name}")
    
    # Verificar que sea un archivo Excel
    if not file_name.lower().endswith(('.xlsx', '.xls')):
        print(f"El archivo {file_name} no es un archivo Excel. Saltando procesamiento.")
        return
    
    try:
        # Leer archivo desde Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        
        # Descargar archivo en memoria
        file_content = blob.download_as_bytes()
        
        # Leer Excel desde memoria
        data = pd.read_excel(io.BytesIO(file_content))
        
        # Limpiar nombres de columnas
        data.rename(columns={'AFC':'AFC_'}, inplace=True)
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
        
        # Crear columnas de egreso e ingreso
        data['egreso'] = 0
        data['ingreso'] = 0
        
        # Calcular egresos e ingresos
        if 'fecha_de_finiquitohistorial' in data.columns and 'periodo' in data.columns:
            data.loc[data['fecha_de_finiquitohistorial'].dt.strftime('%Y-%m').str.replace('-', '') == data['periodo'].astype(str), 'egreso'] = 1
        
        if 'fecha_de_ingreso' in data.columns and 'periodo' in data.columns:
            data.loc[data['fecha_de_ingreso'].dt.strftime('%Y-%m').str.replace('-', '') == data['periodo'].astype(str), 'ingreso'] = 1
        
        # Seleccionar columnas específicas
        columnas_finales = ['rut_dv','fecha_de_ingreso','fecha_de_finiquitohistorial','fecha_de_finiquitohoy',
                          'descripcion_causal','cc','cliente','sector','instalacion','cargo','jornada',
                          'tipo_contrato','total_imponible_hora_extra','egreso','ingreso']
        
        # Filtrar solo las columnas que existen
        columnas_existentes = [col for col in columnas_finales if col in data.columns]
        data = data[columnas_existentes]
        
        data = data.reset_index(drop=True)
        
        # Cargar a BigQuery
        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        )
        
        job = client.load_table_from_dataframe(data, table_id, job_config=job_config)
        job.result()
        
        print(f"Archivo {file_name} procesado exitosamente. {len(data)} registros cargados a BigQuery.")
        
    except Exception as e:
        print(f"Error procesando archivo {file_name}: {str(e)}")
        raise e
