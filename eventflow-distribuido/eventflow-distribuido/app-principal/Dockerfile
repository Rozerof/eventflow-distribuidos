# Usamos una imagen base de Python
FROM python:3.11-slim

# Instalar dependencias de compilación y librerías nativas para psycopg2 y redis
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        python3-dev \
        gcc && \
    rm -rf /var/lib/apt/lists/*

# Establece el directorio de trabajo
WORKDIR /app

# Copia e instala las dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código de la aplicación (que ahora es main.py)
COPY . .

# Exponer el puerto
EXPOSE 8000 

# Comando de inicio: Uvicorn debe apuntar al archivo main.py
# Formato: [nombre_archivo_sin_py]:[nombre_variable_FastAPI]
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
