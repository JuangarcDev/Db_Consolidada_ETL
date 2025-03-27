"""
Script con estructura actualizada de ETL entre DB consolidada y Aplicativo de Chatbot
"""

import psycopg2

# Configuración de conexión a PostgreSQL (modifica según tu entorno)
DB_CHATBOT = {
    "dbname": "Chatbot_Backup_26032025",
    "user": "postgres",
    "password": "1234jcgg",
    "host": "localhost",
    "port": "5432"
}

DB_CONSOLIDADA = {
    "dbname": "Acc_Atencion_Usuarios_Consolidada",
    "user": "postgres",
    "password": "1234jcgg",
    "host": "localhost",
    "port": "5432"
}

def limpiar_telefono(numero):
    """Elimina el prefijo 57 si está presente en el número de teléfono."""
    return numero[2:] if numero and numero.startswith("57") else numero

def etl_chatbot_to_consolidada():
    """Ejecuta la ETL desde la base de datos CHATBOT hacia la consolidada."""
    try:
        # Conectar a las bases de datos
        conn_chatbot = psycopg2.connect(**DB_CHATBOT)
        conn_consolidada = psycopg2.connect(**DB_CONSOLIDADA)
        cursor_chatbot = conn_chatbot.cursor()
        cursor_consolidada = conn_consolidada.cursor()

        # 1. Extraer datos únicos de la BD CHATBOT
        cursor_chatbot.execute("""
            SELECT DISTINCT ON (document_number) document_type, document_number, name, phone_number, created_at
            FROM public.users
            WHERE LENGTH(document_number) <= 30
            ORDER BY document_number, created_at DESC;
        """
        )
        registros_chatbot = cursor_chatbot.fetchall()
        
        # Obtener lista de documentos ya existentes en la consolidada
        cursor_consolidada.execute("SELECT numero_documento FROM public.usuario;")
        documentos_existentes = {row[0] for row in cursor_consolidada.fetchall()}

        nuevos_registros = []
        registros_para_actualizar = []

        for tipo_doc, num_doc, nombre, telefono, fecha_registro in registros_chatbot:
            telefono_limpio = limpiar_telefono(telefono) if telefono else None
            
            if num_doc in documentos_existentes:
                # Actualizar registros existentes
                registros_para_actualizar.append((nombre, telefono_limpio, tipo_doc, fecha_registro, num_doc))
            else:
                # Insertar nuevos registros
                nuevos_registros.append((tipo_doc, num_doc, nombre, telefono_limpio, fecha_registro, "Chatbot"))

        # 2. Insertar o actualizar registros
        if nuevos_registros:
            cursor_consolidada.executemany("""
                INSERT INTO public.usuario (tipo_documento, numero_documento, nombre, telefono, fecha_registro, fuente)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (numero_documento) DO UPDATE
                SET nombre = COALESCE(EXCLUDED.nombre, usuario.nombre),
                    telefono = COALESCE(EXCLUDED.telefono, usuario.telefono),
                    tipo_documento = COALESCE(EXCLUDED.tipo_documento, usuario.tipo_documento),
                    fecha_registro = LEAST(EXCLUDED.fecha_registro, usuario.fecha_registro),
                    fuente = CASE 
                                WHEN usuario.fuente LIKE '%%Chatbot%%' THEN usuario.fuente
                                ELSE usuario.fuente || ', Chatbot'
                             END;
            """, nuevos_registros)

        # 3. Actualizar registros existentes
        if registros_para_actualizar:
            for nombre, telefono, tipo_doc, fecha_registro, num_doc in registros_para_actualizar:
                cursor_consolidada.execute("""
                    UPDATE usuario
                    SET nombre = COALESCE(usuario.nombre, %s),
                        telefono = COALESCE(usuario.telefono, %s),
                        tipo_documento = COALESCE(usuario.tipo_documento, %s),
                        fecha_registro = LEAST(usuario.fecha_registro, %s),
                        fuente = CASE 
                                    WHEN usuario.fuente LIKE '%%Chatbot%%' THEN usuario.fuente
                                    ELSE usuario.fuente || ', Chatbot'
                                 END
                    WHERE numero_documento = %s;
                """, (nombre, telefono, tipo_doc, fecha_registro, num_doc))

        # Confirmar cambios
        conn_consolidada.commit()
        print(f"✅ {len(nuevos_registros)} registros insertados y {len(registros_para_actualizar)} registros actualizados.")
    
    except Exception as e:
        print(f"❌ Error durante la ETL: {e}")
        conn_consolidada.rollback()
    
    finally:
        cursor_chatbot.close()
        cursor_consolidada.close()
        conn_chatbot.close()
        conn_consolidada.close()

if __name__ == "__main__":
    etl_chatbot_to_consolidada()