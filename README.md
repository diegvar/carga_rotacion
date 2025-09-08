# Cloud Function - Sincronización de Rotación

Esta Cloud Function sincroniza datos de rotación de empleados desde una API externa hacia BigQuery.

## Archivos incluidos

- `main.py` - Función principal de la Cloud Function
- `requirements.txt` - Dependencias de Python
- `.gcloudignore` - Archivos a ignorar en el despliegue
- `README.md` - Este archivo

## Variables de entorno requeridas

Configura las siguientes variables de entorno en tu Cloud Function:

- `API_LOCAL_URL` - URL de la API de ControlRoll
- `PROJECT_ID` - ID del proyecto de GCP
- `DATASET_ID` - ID del dataset de BigQuery
- `TABLE_ID` - ID de la tabla de BigQuery
- `TOKEN_CR` - Token de autenticación para la API

## Despliegue desde GitHub

### Opción 1: Usando Google Cloud Console

1. Ve a [Google Cloud Console](https://console.cloud.google.com/)
2. Navega a Cloud Functions
3. Haz clic en "Crear función"
4. Configura:
   - **Nombre**: `rotacion-sync`
   - **Región**: Selecciona tu región preferida
   - **Tipo de activador**: HTTP
   - **Autenticación**: Permitir tráfico no autenticado (si es necesario)
5. En "Código fuente":
   - Selecciona "Repositorio de código fuente"
   - Conecta tu repositorio de GitHub
   - Selecciona la rama y directorio
6. En "Runtime":
   - **Runtime**: Python 3.11
   - **Punto de entrada**: `rotacion_sync`
7. En "Variables de entorno":
   - Agrega todas las variables requeridas
8. Haz clic en "Desplegar"

### Opción 2: Usando gcloud CLI

```bash
# Clona el repositorio
git clone <tu-repositorio-github>
cd <directorio-del-proyecto>

# Despliega la función
gcloud functions deploy rotacion-sync \
  --runtime python311 \
  --trigger-http \
  --allow-unauthenticated \
  --source . \
  --entry-point rotacion_sync \
  --set-env-vars API_LOCAL_URL="tu-api-url",PROJECT_ID="tu-project-id",DATASET_ID="tu-dataset",TABLE_ID="tu-tabla",TOKEN_CR="tu-token"
```

## Uso

Una vez desplegada, la función estará disponible en:
```
https://REGION-PROJECT_ID.cloudfunctions.net/rotacion-sync
```

### Ejemplo de llamada HTTP

```bash
curl -X POST https://REGION-PROJECT_ID.cloudfunctions.net/rotacion-sync
```

### Respuesta exitosa

```json
{
  "success": true,
  "message": "Data procesada exitosamente",
  "records_processed": 1234
}
```

### Respuesta de error

```json
{
  "success": false,
  "error": "Descripción del error",
  "message": "Error al procesar la sincronización"
}
```

## Permisos requeridos

Asegúrate de que la Cloud Function tenga los siguientes permisos de IAM:

- `bigquery.dataEditor` - Para escribir datos en BigQuery
- `bigquery.jobUser` - Para ejecutar trabajos de BigQuery

## Monitoreo

Puedes monitorear la función en:
- Cloud Functions Console
- Cloud Logging
- Cloud Monitoring

## Estructura de datos

La función procesa datos de empleados y genera una tabla con las siguientes columnas principales:

- `period` - Período (YYYY-MM)
- `rut` - RUT del empleado
- `cliente` - Cliente
- `instalacion` - Instalación
- `cecos` - Centro de costos
- `cargo` - Cargo
- `nombre_completo` - Nombre completo
- `estado` - Estado del empleado
- `active_days` - Días activos en el mes
- `active_ratio` - Ratio de actividad
- `hire_in_month` - Contratado en el mes
- `term_in_month` - Terminado en el mes
