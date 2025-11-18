import os
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.pool import SimpleConnectionPool
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from passlib.context import CryptContext
from jose import JWTError, jwt
from datetime import datetime, timedelta
import sys
import time
import requests
import pika
import json

# --- Configuration ---
DB_USER = os.getenv("DB_USER", "user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_HOST = os.getenv("DB_HOST", "db")
DB_NAME = os.getenv("DB_NAME", "eventflow_db")
DB_PORT = os.getenv("DB_PORT", "5432")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Security Constants ---
SECRET_KEY = "a_very_secret_key"
ALGORITHM = "HS256" 
ACCESS_TOKEN_EXPIRE_MINUTES = 30
BCRYPT_MAX_BYTES = 72

# --- Passlib Context ---
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

# --- Funciones de Espera y Chequeo ---

def wait_for_auth_schema(conn, max_attempts=40, delay=2):
    """
    Espera activamente a que la tabla 'users' exista.
    Usa conn.rollback() para limpiar el estado de la conexión en caso de fallo.
    """
    for attempt in range(max_attempts):
        try:
            with conn.cursor() as cursor:
                # Verificar la tabla 'users'
                cursor.execute("SELECT 1 FROM users LIMIT 1;")
            print("INFO: Esquema de autenticación (tabla users) confirmado.")
            return True
        except psycopg2.Error as e:
            # Código 42P01: relation "users" does not exist
            if e.pgcode == '42P01':
                conn.rollback() # Limpiar la conexión abortada
                print(f"WARNING: Esquema AUTH no listo (intento {attempt + 1}/{max_attempts}). Esperando {delay}s...")
                time.sleep(delay)
            else:
                # Si es un error permanente, salimos.
                raise e
    print("CRITICAL ERROR: El esquema de autenticación DB no estuvo listo a tiempo. Inicializando servidor...")
    # Permite que el servidor se inicie aunque las peticiones fallen temporalmente.

def get_db_connection_temp():
    """Establece una conexión simple temporal (sin usar el pool)."""
    return psycopg2.connect(DATABASE_URL)

# --- Database Connection Pool ---
db_pool = None 
try:
    print(f"INFO: Intentando inicializar DB pool en: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    # 1. ESPERA ACTIVA: Usar la conexión temporal para el chequeo del esquema
    for attempt in range(40):
        try:
            conn_temp = get_db_connection_temp()
            # Pasa la conexión para el chequeo de esquema.
            wait_for_auth_schema(conn_temp) 
            conn_temp.close()
            break # Salir del bucle si la tabla existe
        except psycopg2.Error:
            # Capturar errores de UndefinedTable o TransactionAborted
            if attempt == 39:
                print("CRITICAL: El chequeo de esquema falló después de 40 intentos.")
                break # Sale del bucle para que el pool se cree y el servidor inicie.
            time.sleep(2)
        except Exception:
            # Capturar errores de conexión inicial (si la DB no está encendida aún)
            time.sleep(2)
    
    # 2. CREAR EL POOL UNA VEZ QUE EL SERVIDOR DE LA DB ESTÉ LISTO
    db_pool = SimpleConnectionPool(minconn=1, maxconn=10, dsn=DATABASE_URL)
    print("INFO: Pool de conexión a la base de datos inicializado exitosamente.")

except Exception as e:
    print("-" * 50)
    print(f"CRITICAL ERROR: Falló la inicialización final del servicio: {e}")
    print("-" * 50)
    sys.exit(1)

# --- FastAPI App Setup ---
app = FastAPI()

# --- CORS Middleware ---
origins = ["*"] 
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Database Dependency Injection ---
def get_db_connection():
    conn = db_pool.getconn()
    try:
        yield conn
    finally:
        db_pool.putconn(conn)


# --- Models ---
class UserCreate(BaseModel):
    username: str
    email: str
    password: str

class UserLogin(BaseModel):
    username: str
    password: str
class Token(BaseModel):
    access_token: str
    token_type: str
    user_id: int
    username: str

# --- Security Functions ---

def get_truncated_password(password: str) -> str: 
    """Trunca la contraseña a 72 bytes (límite de bcrypt)."""
    password_bytes = password.encode('utf-8')
    if len(password_bytes) > BCRYPT_MAX_BYTES:
        print(f"WARNING: Contraseña truncada de {len(password_bytes)} bytes a {BCRYPT_MAX_BYTES} bytes.")
        return password_bytes[:BCRYPT_MAX_BYTES].decode('utf-8', errors='ignore')
    return password

def verify_password(plain_password, hashed_password):
    plain_password = get_truncated_password(plain_password)
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    password = get_truncated_password(password)
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# --- Endpoints ---
@app.post("/signup", status_code=status.HTTP_201_CREATED, response_model=Token)
def signup(user: UserCreate, conn: any = Depends(get_db_connection)):
    try:
        hashed_password = get_password_hash(user.password)
    except Exception as e:
        print(f"ERROR al hashear la contraseña: {e}")
        raise HTTPException(status_code=500, detail="Error en el procesamiento de la contraseña")
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO users (username, email, hashed_password) VALUES (%s, %s, %s) RETURNING id, username",
                (user.username, user.email, hashed_password)
            )
            new_user = cursor.fetchone()
            conn.commit()
    except psycopg2.IntegrityError:
        raise HTTPException(status_code=400, detail="Username or email already registered")
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error during signup: {e}")
    
    try:
        rmq_host = os.getenv("RABBITMQ_HOST", "queue")

        rmq_connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=rmq_host)
        )
        channel = rmq_connection.channel()
        channel.queue_declare(queue="mail_queue", durable=True)

        email_payload = {
            "email": user.email,
            "subject": "🎉 Bienvenido a EventFlow",
            "message": (
                f"<h1>Hola {user.username},</h1>"
                "<p>Tu cuenta ha sido creada exitosamente.</p>"
            )
        }

        channel.basic_publish(
            exchange="",
            routing_key="mail_queue",
            body=json.dumps(email_payload),
            properties=pika.BasicProperties(
                delivery_mode=2  # Persistente
            ),
        )

        rmq_connection.close()

        print("[AUTH] Correo de bienvenida enviado a mail-sender.")

    except Exception as e:
        print(f"[AUTH] ERROR enviando correo al mail-sender: {e}")

    user_id, username = new_user
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(data={"sub": username, "user_id": user_id}, expires_delta=access_token_expires)
    return {"access_token": access_token, "token_type": "bearer", "user_id": user_id, "username": username}

@app.post("/login", response_model=Token)
def login(user_login: UserLogin, conn: any = Depends(get_db_connection)):
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT * FROM users WHERE username = %s", (user_login.username,))
            user = cursor.fetchone()
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error during login: {e}")


    if not user or not verify_password(user_login.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user["username"], "user_id": user["id"]}, expires_delta=access_token_expires
    )
    
    return {"access_token": access_token, "token_type": "bearer", "user_id": user["id"], "username": user["username"]}

# --- Email Notification Function ---
MAIL_SENDER_URL = os.getenv("MAIL_SENDER_URL", "http://mail-sender:8000/send-email")

def send_welcome_email(email: str, username: str):
    """
    Envía una solicitud al microservicio mail-sender para enviar un correo de bienvenida.
    """
    data = {
        "to_email": email,
        "subject": "Bienvenido a EventFlow",
        "message": f"Hola {username}, ¡tu cuenta ha sido creada exitosamente!"
    }

    try:
        requests.post(MAIL_SENDER_URL, json=data, timeout=5)
    except Exception as e:
        print(f"WARNING: No se pudo enviar correo de bienvenida: {e}")