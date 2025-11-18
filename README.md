-----

# EventFlow: Sistema Distribuido de Venta de Boletos (SPOF Resiliente)

## Descripcion General del Proyecto

EventFlow es una aplicacion de reserva y venta de boletos enfocada en la alta disponibilidad y tolerancia a fallos. El proyecto demuestra resiliencia ante la caida intencional de su base de datos principal (SPOF).

Utilizamos Redis para el bloqueo de concurrencia y RabbitMQ para desacoplar el proceso de compra de la persistencia final de datos.

## Arquitectura y Tecnologias

El sistema consta de 10 componentes desplegados con Docker Compose:

| Componente | Tecnologia | Proposito |
| :--- | :--- | :--- |
| GUI / Frontend | HTML5, JavaScript, Tailwind | Interfaz interactiva de reserva. |
| Nodos de Aplicacion (x3) | FastAPI (Python) | Logica de negocio: Bloqueo de asientos y Orquestacion de Compra. |
| Microservicio Auth | FastAPI (Python) | Gestion de usuarios y registro (APIs REST). |
| Microservicio Consumer | FastAPI (Python) + Pika | Consumidor asincrono. Registra la transaccion final en la DB. |
| Balanceador | NGINX | Distribuye el trafico (Round Robin) entre los 3 nodos. |
| Base de Datos (SPOF) | PostgreSQL | Base de datos persistente unica. (Punto de fallo intencional). |
| Cache / Concurrencia | Redis | Bloqueo de asientos atomico y servicio de lectura rapida. |
| Cola de Eventos | RabbitMQ | Almacena escrituras criticas (compras) de forma asincrona. |
| Monitoreo | Prometheus | Recoleccion de metricas de disponibilidad y latencia. |
| Visualizacion | Grafana | Visualizacion de metricas y estado del sistema. |

## Configuracion y Despliegue

### Requisitos Previos

  * Docker y Docker Compose (V2).

### Pasos de Inicializacion

1.  **Asegurar que la DB este vacia:** Es necesario haber ejecutado `docker-compose down -v` en el pasado.

2.  **Ejecutar el script SQL (Manual):** Esto es crucial para crear las tablas.

    ```bash
    # 1. Copiar el archivo (Asumiendo init.sql en la raiz)
    docker cp .\init.sql postgres_spof:/tmp/init.sql

    # 2. Ejecutar el script dentro del contenedor PostgreSQL
    docker exec postgres_spof psql -U user -d eventflow_db -f /tmp/init.sql
    ```

3.  **Iniciar el Stack Completo:**

    ```bash
    docker-compose up --build -d
    ```

### Acceso a la Aplicacion

| Servicio | URL |
| :--- | :--- |
| Aplicacion Principal | http://localhost:8888/ |
| Monitoreo (Grafana) | http://localhost:3000/ |

## Alta Disponibilidad y Tolerancia a Fallos

El requisito central es la tolerancia al fallo de la BD (SPOF). EventFlow lo cumple con la estrategia de Cache-Aside y Colas Asincronas:

  * **Lecturas Resilientes:** Si PostgreSQL cae, las peticiones GET para el mapa de asientos se sirven desde **Redis** (mecanismo Cache-Aside).
  * **Escrituras Asincronas:** El proceso de compra (`/api/purchase`) publica los datos de la transaccion a **RabbitMQ**. El servicio `ms-consumer` se encarga de registrarlos en PostgreSQL cuando la BD este nuevamente disponible, garantizando que ninguna compra se pierda.
  * **Concurrencia:** **Redis Transactions** se utiliza para el bloqueo atomico, evitando la doble venta de asientos.

## Pruebas y CI/CD

Los tests unitarios estan desarrollados en Python usando `pytest` para verificar la logica de autenticacion y concurrencia.

### Tests Unitarios (7 Cubiertos)

Verifican: Hashing de contrasenas, validacion de contrasenas truncadas, y creacion/decodificacion de tokens JWT.

### Test de Integracion (Ciclo de Vida)

El test de integracion cubre el ciclo completo: **Registro -\> Bloqueo de Asientos (Redis) -\> Intento de Conflicto (409) -\> Compra (RabbitMQ).**
