import pika
import json
import smtplib
from email.mime.text import MIMEText
import os
import time

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "queue")

MAIL_USER = os.environ.get("MAIL_USER")
MAIL_PASS = os.environ.get("MAIL_PASS")
MAIL_SMTP = os.environ.get("MAIL_SMTP", "smtp.gmail.com")
MAIL_PORT = int(os.environ.get("MAIL_PORT", 587))

def send_email(to_email, subject, message):
    try:
        msg = MIMEText(message, "html")
        msg["From"] = MAIL_USER
        msg["To"] = to_email
        msg["Subject"] = subject

        with smtplib.SMTP(MAIL_SMTP, MAIL_PORT) as server:
            server.starttls()
            server.login(MAIL_USER, MAIL_PASS)
            server.sendmail(MAIL_USER, to_email, msg.as_string())

        print(f"[MAIL] Email enviado a {to_email}")
    except Exception as e:
        print(f"[MAIL ERROR] No se pudo enviar email a {to_email}: {e}")


def callback(ch, method, properties, body):
    try:
        data = json.loads(body)

        email = data["email"]
        subject = data["subject"]
        message = data["message"]

        print(f"[MAIL] Enviando correo a {email}...")
        send_email(email, subject, message)

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"[MAIL ERROR] Falló el procesamiento del mensaje: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)  
        # no reintentar para evitar loops


def main():
    print("[MAIL] Iniciando mail-sender...")

    while True:
        try:
            print("[MAIL] Conectando a RabbitMQ...")
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
            )

            channel = connection.channel()
            channel.queue_declare(queue="mail_queue", durable=True)

            print("[MAIL] Esperando mensajes en mail_queue...")
            channel.basic_consume(
                queue="mail_queue",
                on_message_callback=callback
            )

            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError: # type: ignore
            print("[MAIL] ❌ No se pudo conectar a RabbitMQ. Reintentando en 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"[MAIL] ERROR inesperado: {e}. Reintentando en 5s...")
            time.sleep(5)



if __name__ == "__main__":
    main()