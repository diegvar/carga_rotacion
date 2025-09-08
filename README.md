# Cloud Run - Sincronización de Rotación

Este servicio de Cloud Run sincroniza datos de rotación de empleados desde una API externa hacia BigQuery.

## Archivos incluidos

- `main.py` - Aplicación FastAPI principal
- `requirements.txt` - Dependencias de Python
- `Dockerfile` - Configuración de Docker para Cloud Run
- `.dockerignore` - Archivos a ignorar en el build de Docker
- `deploy.sh` - Script de despliegue automatizado
- `config.example` - Ejemplo de configuración de variables de entorno
- `README.md` - Este archivo

## Variables de entorno requeridas

Configura las siguientes variables de entorno en tu servicio de Cloud Run:

- `API_LOCAL_URL` - URL de la API de ControlRoll
- `PROJECT_ID` - ID del proyecto de GCP
- `DATASET_ID` - ID del dataset de BigQuery
- `TABLE_ID` - ID de la tabla de BigQuery
- `TOKEN_CR` - Token de autenticación para la API

## Despliegue a Cloud Run

### Opción 1: Usando el script automatizado

1. Configura las variables de entorno en el archivo `config.example`
2. Ejecuta el script de despliegue:
```bash
./deploy.sh
```

### Opción 2: Usando Google Cloud Console

1. Ve a [Google Cloud Console](https://console.cloud.google.com/)
2. Navega a Cloud Run
3. Haz clic en "Crear servicio"
4. Configura:
   - **Nombre**: `carga-rotacion`
   - **Región**: `us-east1`
   - **Autenticación**: Permitir tráfico no autenticado
5. En "Código fuente":
   - Selecciona "Repositorio de código fuente"
   - Conecta tu repositorio de GitHub
   - Selecciona la rama y directorio
6. En "Variables de entorno":
   - Agrega todas las variables requeridas
7. Haz clic en "Crear"

### Opción 3: Usando gcloud CLI

```bash
# Construir y desplegar
gcloud builds submit --tag gcr.io/pruebas-463316/carga-rotacion
gcloud run deploy carga-rotacion \
  --image gcr.io/pruebas-463316/carga-rotacion \
  --platform managed \
  --region us-east1 \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --timeout 3600 \
  --set-env-vars API_LOCAL_URL="tu-api-url",PROJECT_ID="pruebas-463316",DATASET_ID="tu-dataset",TABLE_ID="tu-tabla",TOKEN_CR="tu-token"
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
