import psycopg2

# Configuración de conexión a PostgreSQL (modifica según tu entorno)
DB_SOURCE = {
    "dbname": "Chatbot_Backup_26032025",
    "user": "postgres",
    "password": "1234jcgg",
    "host": "localhost",
    "port": "5432"
}

DB_TARGET = {
    "dbname": "Acc_Atencion_Usuarios_Consolidada",
    "user": "postgres",
    "password": "1234jcgg",
    "host": "localhost",
    "port": "5432"
}

def transformar_telefono(phone):
    if phone.startswith("57") and len(phone) > 2:
        return phone[2:]
    return phone

def migrar_datos_chatbot():
    try:
        # Conectar a las bases de datos
        conn_source = psycopg2.connect(**DB_SOURCE)
        cursor_source = conn_source.cursor()
        
        conn_target = psycopg2.connect(**DB_TARGET)
        cursor_target = conn_target.cursor()
        
        # Extraer datos de la fuente
        cursor_source.execute("""
            SELECT name, phone_number, document_number, created_at
            FROM users
            WHERE document_type = 'CC' AND LENGTH(document_number) <= 20
        """)
        registros = cursor_source.fetchall()
        
        registros_transformados = []
        for nombre, telefono, cedula, fecha_registro in registros:
            telefono = transformar_telefono(telefono)
            registros_transformados.append((cedula, nombre, telefono, fecha_registro, 'CHATBOT'))
        
        # Insertar datos en la tabla consolidada evitando duplicados en Fuente
        for registro in registros_transformados:
            cursor_target.execute("""
                INSERT INTO Usuario (Cedula, Nombre, Telefono, Fecha_Registro, Fuente)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (Cedula) DO UPDATE 
                SET Nombre = COALESCE(EXCLUDED.Nombre, Usuario.Nombre),
                    Telefono = COALESCE(EXCLUDED.Telefono, Usuario.Telefono),
                    Fecha_Registro = COALESCE(EXCLUDED.Fecha_Registro, Usuario.Fecha_Registro),
                    Fuente = CASE 
                                WHEN Usuario.Fuente LIKE '%CHATBOT%' THEN Usuario.Fuente 
                                ELSE Usuario.Fuente || ', CHATBOT' 
                             END;
            """, registro)
        
        conn_target.commit()
        print("Migración desde CHATBOT completada exitosamente.")
    
    except Exception as e:
        print("Error durante la migración:", e)
    
    finally:
        cursor_source.close()
        conn_source.close()
        cursor_target.close()
        conn_target.close()

if __name__ == "__main__":
    migrar_datos_chatbot()
