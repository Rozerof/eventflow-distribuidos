import os
import sys
import json
import uuid
import time
import redis
import pika
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
from prometheus_fastapi_instrumentator import Instrumentator

# --- 1. Configuration & Environment Variables ---
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "eventflow_db")
DB_PORT = os.getenv("DB_PORT", "5432")
NODE_ID = os.getenv("NODE_ID", "default")

REDIS_HOST = os.getenv("REDIS_HOST", "cache")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "queue")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))

# Constantes de robustez
MAX_RETRIES = 10
RETRY_DELAY = 5 # segundos
SCHEMA_CHECK_ATTEMPTS = 100 

# Constante para la expiración de bloqueos de asientos (en segundos)
LOCK_EXPIRATION_SECONDS = 300 
LOCK_KEY_PREFIX = "lock:" 
EVENT_KEY_PREFIX = "event:" 
SEAT_KEY_PREFIX = "seat:" 

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- 2. Initialization (Conexiones con reintentos) ---

db_conn = None
# Conexión a PostgreSQL
for attempt in range(MAX_RETRIES):
    try:
        db_conn = psycopg2.connect(DATABASE_URL)
        db_conn.autocommit = True
        print("INFO: Conexión exitosa a PostgreSQL.")
        break
    except Exception as e:
        print(f"WARNING: Falló el intento {attempt + 1} de conexión a PostgreSQL. Reintentando en {RETRY_DELAY}s...")
        time.sleep(RETRY_DELAY)
else:
    # BLINDAJE: Permitir que el servidor inicie aunque falle la conexión inicial
    print(f"WARNING: Falló la conexión inicial a PostgreSQL después de {MAX_RETRIES} intentos. Iniciando servidor...")


# Conexión a Redis
redis_client = None
for attempt in range(MAX_RETRIES):
    try:
        redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        redis_client.ping()
        print("INFO: Conexión exitosa a Redis.")
        break
    except Exception as e:
        print(f"WARNING: Falló el intento {attempt + 1} de conexión a Redis. Reintentando en {RETRY_DELAY}s...")
        time.sleep(RETRY_DELAY)
else:
    # BLINDAJE
    print(f"WARNING: Falló la conexión inicial a Redis después de {MAX_RETRIES} intentos. Iniciando servidor...")

# Conexión a RabbitMQ
rabbitmq_connection = None
rabbitmq_channel = None
for attempt in range(MAX_RETRIES):
    try:
        rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT))
        rabbitmq_channel = rabbitmq_connection.channel()
        rabbitmq_channel.queue_declare(queue='transaction_queue', durable=True) 
        print("INFO: Conexión exitosa a RabbitMQ.")
        break
    except Exception as e:
        print(f"WARNING: Falló el intento {attempt + 1} de conexión a RabbitMQ. Reintentando en {RETRY_DELAY}s...")
        time.sleep(RETRY_DELAY)
else:
    # BLINDAJE
    print(f"WARNING: Falló la conexión inicial a RabbitMQ después de {MAX_RETRIES} intentos. Iniciando servidor...")

app = FastAPI(title=f"EventFlow App Node {NODE_ID}")

# --- 3. CORS and Monitoring ---

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Instrumentación fuera del startup event.
Instrumentator().instrument(app).expose(app, endpoint="/metrics")

# --- FUNCIÓN CRÍTICA DE ESPERA DE ESQUEMA ---
def wait_for_db_schema(conn, max_attempts=SCHEMA_CHECK_ATTEMPTS, delay=2):
    """
    Espera activamente a que la tabla 'events' exista.
    Ahora no contiene sys.exit(1).
    """
    for attempt in range(max_attempts):
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM events LIMIT 1;")
            print("INFO: Esquema de la base de datos (tabla events) confirmado.")
            return True
        except psycopg2.Error as e:
            if e.pgcode == '42P01':
                conn.rollback() 
                print(f"WARNING: Esquema DB no listo (intento {attempt + 1}/{max_attempts}). Esperando {delay}s...")
                time.sleep(delay)
            else:
                raise e
    print("WARNING: El esquema DB no estuvo listo a tiempo. El servidor comenzará, pero las peticiones fallarán hasta que la DB esté lista.")
    # ELIMINADO: sys.exit(1)
    return False

@app.on_event("startup")
async def startup_event():
    pass

# --- 4. Database & Cache Helpers ---

def get_event_from_db(event_id):
    """Obtiene información del evento desde la base de datos."""
    # Comprobación de conexión antes de usar db_conn
    if db_conn is None:
        raise HTTPException(status_code=503, detail="Database connection is not ready.")
    
    with db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("SELECT id, name, description, date FROM events WHERE id = %s", (event_id,))
        event = cursor.fetchone()
        return event

def load_event_to_cache(event_id):
    """Carga y precarga un evento dummy de 100 asientos en Redis si no existe."""
    # Comprobación de conexión antes de usar db_conn
    if db_conn is None:
        raise HTTPException(status_code=503, detail="Database connection is not ready for preload.")
        
    event_key = f"{EVENT_KEY_PREFIX}{event_id}"
    
    if redis_client is None or redis_client.exists(event_key):
        print(f"INFO: Evento {event_id} encontrado en Redis.")
        return

    event_data = get_event_from_db(event_id)
    if not event_data:
        raise HTTPException(status_code=404, detail="Event not found in DB.")

    seat_map = {}
    for row in range(1, 11):
        for col in range(1, 11):
            seat_id = f"{chr(64 + row)}{col}"
            seat_map[seat_id] = "available" 
    
    redis_client.hmset(event_key, seat_map)
    print(f"INFO: Evento {event_id} precargado en Redis.")
    
    redis_client.set(f"metadata:{event_id}", json.dumps(event_data))


# --- LÓGICA DE INICIALIZACIÓN DE DATOS (CRÍTICA) ---

# 1. Esperamos a que el esquema esté listo (Si db_conn no es None)
if db_conn is not None:
    wait_for_db_schema(db_conn) 

# 2. Intentamos la precarga de datos de prueba
try:
    if db_conn is not None:
        load_event_to_cache(999) 
        print(f"INFO: Evento 999 precargado en Redis.")
except Exception as e:
    print(f"WARNING: Falló la precarga del evento de prueba: {e}") 

# --- 5. Models, 6. Event Publisher, 7. Endpoints, 8. Helpers, 9. Health Check (Sin cambios) ---

class EventSummary(BaseModel):
    id: int
    name: str
    description: str
    date: str

class SeatStatus(BaseModel):
    seat_id: str
    status: str

class LockSeatsRequest(BaseModel):
    event_id: int
    seats: list[str]
    user_id: int

class PurchaseRequest(BaseModel):
    lock_id: str
    user_id: int
    event_id: int 

# --- 6. Event Publisher (RabbitMQ) ---

def publish_purchase_event(purchase_data):
    """Publica un mensaje de compra exitosa a RabbitMQ."""
    global rabbitmq_connection, rabbitmq_channel
    
    try:
        if not rabbitmq_connection or not rabbitmq_connection.is_open:
            rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT))
            rabbitmq_channel = rabbitmq_connection.channel()
            rabbitmq_channel.queue_declare(queue='transaction_queue', durable=True)
        
        if not rabbitmq_channel or rabbitmq_channel.is_closed:
            rabbitmq_channel = rabbitmq_connection.channel()
            rabbitmq_channel.queue_declare(queue='transaction_queue', durable=True)
            
        message = json.dumps(purchase_data)
        rabbitmq_channel.basic_publish(
            exchange='',
            routing_key='transaction_queue',
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            )
        )
        print(f"INFO: Evento de compra publicado a RabbitMQ: {purchase_data['transaction_id']}")
    except Exception as e:
        print(f"CRITICAL ERROR: Falló la publicación a RabbitMQ: {e}")

# --- 7. API Endpoints ---
@app.get("/api/events")
async def get_events():
    """Obtiene la lista de eventos disponibles desde la base de datos."""
    # Verificar si la conexión a la DB está viva
    if db_conn is None:
        raise HTTPException(status_code=503, detail="Service Unavailable: Database connection failed during startup.")
        
    try:
        with db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT id, name, description, date FROM events")
            events = cursor.fetchall()
            
            if not events:
                load_event_to_cache(999) 
                events = get_event_from_db(999)
                if events:
                    events = [events]
            return {"events": events}
    except Exception as e:
        # Esto captura errores de "relation 'events' does not exist"
        raise HTTPException(status_code=500, detail=f"Database error during event retrieval: {e}")

@app.get("/api/events/{event_id}/seats", response_model=list[SeatStatus])
async def get_seat_map(event_id: int):
    """Obtiene el estado de todos los asientos de un evento desde Redis."""
    # Comprobación de conexión a DB antes de usar load_event_to_cache
    if db_conn is None:
        raise HTTPException(status_code=503, detail="Database connection is unavailable.")
        
    event_key = f"{EVENT_KEY_PREFIX}{event_id}"
    
    if not redis_client.exists(event_key):
        load_event_to_cache(event_id)
        if not redis_client.exists(event_key):
             raise HTTPException(status_code=404, detail="Event seat map not found.")

    seat_data = redis_client.hgetall(event_key)
    
    response = []
    for seat_id, status in seat_data.items():
        if status.startswith("locked:"):
            status_display = "locked" 
        else:
            status_display = status
        
        response.append(SeatStatus(seat_id=seat_id, status=status_display))
        
    return response

@app.post("/api/seats/lock")
async def lock_seats(request: LockSeatsRequest):
    """Intenta bloquear asientos usando una transacción atómica de Redis (Pipeline)."""
    if db_conn is None:
        raise HTTPException(status_code=503, detail="Database connection is unavailable.")

    event_key = f"{EVENT_KEY_PREFIX}{request.event_id}"
    user_lock_key = f"{LOCK_KEY_PREFIX}{request.user_id}:{request.event_id}"
    lock_id = str(uuid.uuid4())

    if redis_client.exists(user_lock_key):
        raise HTTPException(status_code=400, detail="User already has seats locked for this event.")

    pipe = redis_client.pipeline()
    current_statuses = redis_client.hmget(event_key, request.seats)
    
    seats_to_lock = {}
    for i, seat_id in enumerate(request.seats):
        current_status = current_statuses[i]
        
        if current_status is None:
             raise HTTPException(status_code=404, detail=f"Seat {seat_id} not found.")
             
        if current_status != 'available':
            raise HTTPException(status_code=409, detail=f"Seat {seat_id} is currently {current_status.split(':')[0]}.")
            
        seats_to_lock[seat_id] = f"locked:{lock_id}"

    try:
        pipe.multi()
        pipe.hmset(event_key, seats_to_lock) 
        
        lock_data = {
            "lock_id": lock_id,
            "user_id": request.user_id,
            "event_id": request.event_id,
            "seats": json.dumps(request.seats)
        }
        pipe.hmset(user_lock_key, lock_data)
        pipe.expire(user_lock_key, LOCK_EXPIRATION_SECONDS)

        pipe.execute()
        
        return {
            "lock_id": lock_id,
            "expires_in": LOCK_EXPIRATION_SECONDS,
            "seats": request.seats
        }
    except redis.exceptions.WatchError:
        raise HTTPException(status_code=409, detail="Concurrency conflict: Seats status changed. Try again.")
    except Exception as e:
        print(f"ERROR during seat lock transaction: {e}")
        raise HTTPException(status_code=500, detail=f"Internal lock error: {e}")

@app.post("/api/purchase")
async def purchase_seats(request: PurchaseRequest):
    """Finaliza el proceso de compra."""
    if db_conn is None:
        raise HTTPException(status_code=503, detail="Database connection is unavailable.")

    user_lock_key = f"{LOCK_KEY_PREFIX}{request.user_id}:{request.event_id}"
    event_key = f"{EVENT_KEY_PREFIX}{request.event_id}"

    lock_data = redis_client.hgetall(user_lock_key)
    if not lock_data or lock_data.get('lock_id') != request.lock_id:
        raise HTTPException(status_code=404, detail="Invalid or expired lock_id.")

    locked_seats = json.loads(lock_data['seats'])

    payment_status = "success" 

    if payment_status != "success":
        redis_client.delete(user_lock_key)
        raise HTTPException(status_code=400, detail="Payment failed. Seats released.")
    
    pipe = redis_client.pipeline()
    try:
        pipe.multi()

        seats_to_mark_taken = {seat: "taken" for seat in locked_seats}
        pipe.hmset(event_key, seats_to_mark_taken)
        
        pipe.delete(user_lock_key)

        pipe.execute()
        
        transaction_id = str(uuid.uuid4())
        
        with db_conn.cursor() as cursor:
            for seat_id in locked_seats:
                cursor.execute(
                    "INSERT INTO purchases (user_id, event_id, seat_id, transaction_id) VALUES (%s, %s, %s, %s)",
                    (request.user_id, request.event_id, seat_id, transaction_id)
                )
            db_conn.commit()
            
        purchase_data = {
            "transaction_id": transaction_id,
            "user_id": request.user_id,
            "event_id": request.event_id,
            "seats": locked_seats,
            "purchase_time": datetime.utcnow().isoformat()
        }
        publish_purchase_event(purchase_data)

        return {
            "message": "Purchase successful. Transaction pending final processing.",
            "transaction_id": transaction_id
        }

    except Exception as e:
        print(f"CRITICAL ERROR during purchase: {e}")
        raise HTTPException(status_code=500, detail=f"Internal processing error: {e}")

# --- 8. Helper para limpiar bloqueos expirados ---
@app.delete("/api/locks/cleanup")
async def cleanup_locks():
    """Endpoint para limpiar bloqueos expirados."""
    return {"message": "Lock cleanup finished (Redis TTL handles expiration)."}

# --- 9. Health Check ---
@app.get("/health")
async def health_check():
    """Verificación básica de salud del nodo."""
    try:
        redis_client.ping()
        
        # Verificar la conexión a DB
        if db_conn is None:
            raise Exception("Database connection is inactive.")
            
        with db_conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            
        pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)).close()
        
        return {"status": "ok", "node_id": NODE_ID, "database": "connected", "cache": "connected", "queue": "connected"}
    except Exception as e:
        print(f"Health Check Failed: {e}")
        raise HTTPException(status_code=503, detail=f"Service unavailable: {e}")