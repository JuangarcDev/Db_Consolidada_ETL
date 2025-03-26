"""
Script con estructura actualizada de ETL entre DB consolidada y Aplicativo de Chatbot
"""

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
    """Transforma el número de teléfono según las reglas definidas"""
    if phone and phone.startswith("57") and len(phone) > 2:
        return phone[2:]
    elif phone and len(phone) > 2:
        return "({}){}".format(phone[:2], phone[2:])
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
            SELECT name, phone_number, document_type, document_number, created_at
            FROM users
            WHERE document_type IN ('CC', 'TI', 'CE')  -- Incluyendo más tipos de documento
              AND LENGTH(document_number) <= 30
        """)
        registros = cursor_source.fetchall()
        
        total_registros = len(registros)
        registros_transformados = []
        omitidos = 0

        for fila in registros:
            try:
                # Validar que la fila tenga exactamente 5 valores
                if len(fila) != 5:
                    print(f"Registro omitido por estructura incorrecta: {fila}")
                    omitidos += 1
                    continue
                
                nombre, telefono, tipo_doc, cedula, fecha_registro = fila
                
                if not cedula:  # Si la cédula es NULL o vacía, se omite
                    print(f"Registro omitido por falta de cédula: {fila}")
                    omitidos += 1
                    continue
                
                telefono = transformar_telefono(telefono)
                registros_transformados.append((cedula, nombre, telefono, tipo_doc, fecha_registro, 'CHATBOT'))
            
            except Exception as e:
                print(f"Error procesando fila {fila}: {e}")
                omitidos += 1
                continue

        # Variables para estadísticas
        nuevos_insertados = 0
        complementados = 0

        # Insertar datos en la tabla consolidada evitando duplicados y complementando información
        for registro in registros_transformados:
            cursor_target.execute("""
                INSERT INTO Usuario (Cedula, Nombre, Telefono, Tipo_Documento, Fecha_Registro, Fuente)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (Cedula) DO UPDATE 
                SET Nombre = COALESCE(EXCLUDED.Nombre, Usuario.Nombre),
                    Telefono = COALESCE(Usuario.Telefono, EXCLUDED.Telefono),  -- Solo actualizar si estaba vacío
                    Tipo_Documento = COALESCE(EXCLUDED.Tipo_Documento, Usuario.Tipo_Documento),
                    Fecha_Registro = LEAST(EXCLUDED.Fecha_Registro, Usuario.Fecha_Registro),  -- Mantener la fecha más antigua
                    Fuente = CASE 
                                WHEN Usuario.Fuente LIKE '%CHATBOT%' THEN Usuario.Fuente 
                                ELSE Usuario.Fuente || ', CHATBOT' 
                             END
                RETURNING (xmax = 0) AS inserted;
            """, registro)

            # Verificar si fue un insert o update
            insertado = cursor_target.fetchone()[0]
            if insertado:
                nuevos_insertados += 1
            else:
                complementados += 1

        conn_target.commit()

        # Resumen del proceso
        print("📊 Resumen de la Migración 📊")
        print(f"Total registros en CHATBOT: {total_registros}")
        print(f"Registros omitidos por error: {omitidos}")
        print(f"Registros insertados nuevos: {nuevos_insertados}")
        print(f"Registros complementados: {complementados}")
        print("✅ Migración completada exitosamente.")

    except Exception as e:
        print("❌ Error durante la migración:", e)

    finally:
        cursor_source.close()
        conn_source.close()
        cursor_target.close()
        conn_target.close()

if __name__ == "__main__":
    migrar_datos_chatbot()
