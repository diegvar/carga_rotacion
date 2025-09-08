# Usar la imagen oficial de Python
FROM python:3.11-slim

# Establecer el directorio de trabajo
WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements.txt e instalar dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la aplicación
COPY main.py .

# Exponer el puerto 8080 (puerto estándar para Cloud Functions)
EXPOSE 8080

# Variables de entorno por defecto (se pueden sobrescribir)
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# Comando para ejecutar la función
CMD exec functions-framework --target=rotacion_sync --port=$PORT --host=0.0.0.0
