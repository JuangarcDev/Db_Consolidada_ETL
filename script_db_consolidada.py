"""
Script con estructura inicial de creación de la DB consolidada, que sea intuitiva y junte los usuarios 
atendidos desde el 2024, de los aplicativos de Tu_Catastro, ChatBot y Digiturno
"""

import psycopg2

# Configuración de la conexión a PostgreSQL
DB_CONFIG = {
    "dbname": "Acc_Atencion_Usuarios_Consolidada",
    "user": "postgres",
    "password": "1234jcgg",
    "host": "localhost",
    "port": "5432"
}

def crear_tabla_usuario():
    try:
        # Conectar a la base de datos
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Crear tabla Usuario con restricciones
        create_table_query = """
        CREATE TABLE Usuario (
            Id SERIAL PRIMARY KEY,
            Tipo_Documento VARCHAR(50),
            Numero_Documento VARCHAR(30) UNIQUE NOT NULL,
            Nombre VARCHAR(100) NOT NULL,
            Telefono VARCHAR(15) CHECK (Telefono ~ '^[0-9]+$'),
            Fecha_Registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            Fuente VARCHAR(50) NOT NULL,
            Completitud VARCHAR(50),
            Municipio VARCHAR(50),
            Email VARCHAR(100) CHECK (Email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
            Zona VARCHAR(50),
            Vereda VARCHAR(100)
        );
        """
        
        # Ejecutar la consulta
        cursor.execute(create_table_query)
        conn.commit()
        print("Tabla 'Usuario' creada exitosamente.")

    except psycopg2.errors.DuplicateTable:
        print("La tabla 'Usuario' ya existe.")
    except Exception as e:
        print("Error al crear la tabla:", e)
    finally:
        cursor.close()
        conn.close()

# Ejecutar la función para crear la tabla
if __name__ == "__main__":
    crear_tabla_usuario()
