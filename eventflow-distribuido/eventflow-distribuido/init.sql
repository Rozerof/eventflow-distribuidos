-- Este script se ejecuta automáticamente cuando el contenedor de PostgreSQL se inicia por primera vez.

-- 1. Tablas del Esquema

-- Tabla para usuarios del sistema de autenticación
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabla para los eventos
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    date TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Tabla para las compras de tickets
CREATE TABLE IF NOT EXISTS purchases (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    event_id INTEGER NOT NULL,
    transaction_id VARCHAR(36) NOT NULL, 
    seat_id VARCHAR(5) NOT NULL, 
    quantity INTEGER DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE
);


-- 2. Inserción de Datos de Ejemplo (Método ON CONFLICT)

-- Usamos ON CONFLICT DO NOTHING en lugar de WHERE NOT EXISTS para un manejo más limpio
-- del conflicto de PKs que a veces interfiere con el entrypoint de Docker.

-- Insertar datos de ejemplo para eventos (ID 999)
INSERT INTO events (id, name, description, date) 
VALUES (999, 'Concierto de Resiliencia', 'Un evento para probar la arquitectura Cache-Aside.', '2025-12-01T20:00:00Z')
ON CONFLICT (id) DO NOTHING;

-- Insertar datos de ejemplo para usuarios (ID 1)
INSERT INTO users (id, username, email, hashed_password) 
VALUES (1, 'testuser', 'test@example.com', '$2b$12$EixZaYVK1fsbw/WBZ3J9A.T6m2w.g2b.j2X.Yg2b.j2X.Yg2b.j2')
ON CONFLICT (id) DO NOTHING;