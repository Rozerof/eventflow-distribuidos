import os
import time
import psycopg2 
import pika 
import json # 👈 Añadido para manejar el cuerpo del mensaje
import threading # 👈 Añadido para ejecutar el consumidor en segundo plano
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import prometheus_fastapi_instrumentator

# --- Configuración del Entorno ---
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "queue")
DB_HOST = os.environ.get("DB_HOST", "db")
DB_USER = os.environ.get("DB_USER", "user") 
DB_PASSWORD = os.environ.get("DB_PASSWORD", "password")
DB_NAME = os.environ.get("DB_NAME", "eventflow_db")
# -----------------------------------------------

# 1. Crear la aplicación FastAPI
app = FastAPI(title="Microservicio Consumidor de Compras")

# --- Habilitar CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. Instrumentar la aplicación para Prometheus
# Esto crea un endpoint /metrics automáticamente
prometheus_fastapi_instrumentator.Instrumentator().instrument(app).expose(app)

# --- Función de Conexión a la BD ---
def get_db_connection():
    """Intenta conectar a la BD usando las variables de entorno."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME, 
            user=DB_USER, 
            password=DB_PASSWORD,
            connect_timeout=3
        )
        return conn
    except Exception as e:
        print(f"CONSUMER DB ERROR: Falló la conexión a PostgreSQL: {e}")
        return None

# --- Función de Conexión a RabbitMQ ---
def get_rabbitmq_connection():
    """Intenta conectar a RabbitMQ."""
    try:
        params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=5672)
        connection = pika.BlockingConnection(params)
        return connection
    except Exception as e:
        return None

# --- Lógica de Procesamiento de Negocio ---
def insert_purchase_record(conn, channel, data):
    """
    Inserta el registro de compra y encola un correo de confirmación.
    
    Args:
        conn: Conexión a la base de datos.
        channel: Canal de RabbitMQ para encolar el correo.
        data: Datos de la compra.

    Returns:
        bool: True si ambas operaciones (BD y encolado) son exitosas, False en caso contrario.
    """
    user_email = None

    try:
        with conn.cursor() as cursor:
            # Esta es una query de ejemplo, asume que existe la tabla 'purchases'
            # y que 'seats' se almacena como un ARRAY o JSONB. Usaremos JSON para simplicidad.
            cursor.execute(
                """
                INSERT INTO purchases (transaction_id, user_id, event_id, seats_data, purchase_time)
                VALUES (%s, %s, %s, %s, to_timestamp(%s))
                """,
                (
                    data['transaction_id'], 
                    data['user_id'], 
                    data['event_id'], 
                    json.dumps(data['seats']), # Convertir la lista de asientos a JSON string
                    data['timestamp']
                )
            )

            # --- Obtener el email del usuario desde la BD ---
            cursor.execute("SELECT email FROM users WHERE id = %s", (data['user_id'],))
            result = cursor.fetchone()
            if result:
                user_email = result[0]
            
            conn.commit()
        print(f"CONSUMER SUCCESS: Compra {data['transaction_id']} registrada en BD.")

    except Exception as e:
        print(f"CONSUMER CRITICAL ERROR: Falló la inserción en BD: {e}")
        conn.rollback() # Revertir cambios si algo falla
        return False
    
    if not user_email:
        print(f"CONSUMER WARNING: No se encontró email para el user_id {data['user_id']}. No se enviará correo.")
        # Decidimos que es un éxito parcial, ya que la compra se guardó.
        # Si el correo fuera crítico, aquí deberíamos retornar False.
        return True

    # --- Enviar correo ---
    try:
        channel.queue_declare(queue="mail_queue", durable=True)

        email_payload = {
            "email": user_email,
            "subject": "✔ Compra Confirmada",
            "message": (
                f"<h1>¡Gracias por tu compra!</h1>"
                f"<p>Evento: {data['event_id']}<br>"
                f"Asientos: {data['seats']}</p>"
            )
        }

        channel.basic_publish(exchange="",
                              routing_key="mail_queue",
                              body=json.dumps(email_payload),
                              properties=pika.BasicProperties(delivery_mode=2))
        print("[CONSUMER] Correo encolado correctamente.")
        return True

    except Exception as e:
        print(f"[CONSUMER] ERROR encolando correo al mail-sender: {e}")
        return False   # <--- ESTE CAMBIO ES CRÍTICO

# --- Callback del Consumidor ---
def process_message(ch, method, properties, body):
    try:
        message_data = json.loads(body)
        print(f"CONSUMER INFO: Procesando TXN: {message_data['transaction_id']}")
        
        conn = get_db_connection()
        
        if conn is None:
            # Si la BD está caída, hacemos NACK para reencolar el mensaje.
            print("CONSUMER WARNING: BD caída. Devolviendo mensaje a la cola (NACK).")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return

        # Intentar insertar el registro de compra
        if insert_purchase_record(conn, ch, message_data):
            ch.basic_ack(delivery_tag=method.delivery_tag) # ACK si la inserción fue exitosa
        else:
            # Si falla la inserción por error de query, podría ser irreparable.
            # En producción, se debería mover a una cola de mensajes fallidos (Dead-Letter Queue).
            print("CONSUMER CRITICAL: Falló la inserción en BD de forma irreparable. Haciendo NACK sin reencolar.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
        conn.close()
        
    except Exception as e:
        print(f"CONSUMER CRITICAL ERROR: Error inesperado en el procesamiento: {e}")
        # En caso de un error de deserialización de JSON, NACK sin reencolar (mensaje corrupto)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

# --- Bucle de Consumo de RabbitMQ ---
def start_consuming():
    """Bucle principal que maneja la conexión y el consumo de RabbitMQ con reintentos."""
    print("CONSUMER THREAD: Iniciando el bucle de consumo de RabbitMQ.")
    while True:
        connection = None
        try:
            connection = get_rabbitmq_connection()
            if connection is None:
                raise pika.exceptions.AMQPConnectionError("No se pudo conectar a RabbitMQ.") # type: ignore
            
            channel = connection.channel()
            channel.queue_declare(queue='transaction_queue', durable=True)
            channel.basic_qos(prefetch_count=1) # Procesa un mensaje a la vez
            
            channel.basic_consume(
                queue='transaction_queue',
                on_message_callback=process_message,
                auto_ack=False # Es vital manejar el ACK/NACK manualmente
            )
            
            print('CONSUMER INFO: Esperando mensajes en transaction_queue...')
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as e: # type: ignore
            print(f"CONSUMER WARNING: Conexión RabbitMQ perdida o fallida: {e}. Reintentando en 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"CONSUMER CRITICAL: Error inesperado en el bucle de consumo: {e}. Reintentando en 5s.")
            time.sleep(5)
        finally:
            if connection and connection.is_open:
                connection.close()

# --- Endpoint y Health Check ---

@app.get("/")
def health_check():
    """Endpoint de salud para verificar que el servicio está vivo y sus dependencias."""
    
    # Prueba rápida de conexión a la BD
    db_status = "down"
    conn = get_db_connection()
    if conn:
        db_status = "up"
        conn.close()
    
    # Prueba rápida de conexión a RabbitMQ
    rabbitmq_status = "down"
    connection = get_rabbitmq_connection()
    if connection:
        rabbitmq_status = "up"
        connection.close()

    return {
        "status": "ok", 
        "service": "ms-consumer", 
        "dependencies": {
            "postgres": db_status, 
            "rabbitmq": rabbitmq_status
        }
    }

# 3. Inicializar el consumidor en un hilo de fondo al iniciar FastAPI
@app.on_event("startup")
def startup_event():     
    """Inicia el consumidor de RabbitMQ en un hilo separado."""
    consumer_thread = threading.Thread(target=start_consuming, daemon=True)
    consumer_thread.start()
    print("FASTAPI INFO: Hilo de consumo de RabbitMQ iniciado en segundo plano.")