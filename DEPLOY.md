# Despliegue de Cloud Function para Procesamiento de Rotación

## Archivos incluidos
- `main.py`: Función principal que procesa archivos Excel
- `requirements.txt`: Dependencias de Python

## Pasos para desplegar

### 1. Crear el bucket de Storage (si no existe)
```bash
gsutil mb gs://tu-bucket-rotacion
```

### 2. Desplegar la Cloud Function
```bash
gcloud functions deploy process-rotacion-file \
  --runtime python311 \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=tu-bucket-rotacion" \
  --source=. \
  --entry-point=process_rotacion_file \
  --project=pruebas-463316 \
  --region=us-central1
```

### 3. Configurar permisos
La Cloud Function necesita los siguientes permisos:
- `storage.objectViewer` en el bucket de origen
- `bigquery.dataEditor` en el dataset de destino
- `bigquery.jobUser` para ejecutar trabajos de carga

### 4. Probar la función
Sube un archivo Excel al bucket configurado y verifica que se procese correctamente.

## Configuración actual
- **Proyecto**: pruebas-463316
- **Dataset**: ww_data_upload_asistencia
- **Tabla**: testdata
- **Trigger**: Cuando se sube un archivo .xlsx o .xls al bucket

## Notas
- La función solo procesa archivos Excel (.xlsx, .xls)
- Los datos se cargan con WRITE_TRUNCATE (sobrescribe la tabla)
- Los logs se pueden ver en Cloud Logging
