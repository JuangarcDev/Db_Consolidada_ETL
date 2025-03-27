"""
SCRIPT INICIAL PARA REALIZAR ESTANDARIZACIÓN Y VERIFICACIÓN DE CAMPOS DE LA DB CONSOLIDADA.
"""
import psycopg2
import re

def limpiar_y_estandarizar():
    try:
        conn = psycopg2.connect(
            dbname='Acc_Atencion_Usuarios_Consolidada', user='postgres', password='1234jcgg', host='localhost', port='5432'
        )
        cursor = conn.cursor()

        # Agregar el campo Numero_Documento_Limpio si no existe
        cursor.execute("""
            ALTER TABLE Usuario ADD COLUMN IF NOT EXISTS Numero_Documento_Limpio VARCHAR(30);
        """)
        conn.commit()

        # Contadores
        total_registros = 0
        registros_actualizados = 0
        registros_eliminados = 0
        registros_sin_cambios = 0
        registros_duplicados = 0
        
        # Estandarizar Tipo_Documento
        map_tipo_doc = {
            'CC': 'Cedula_Ciudadania',
            'CE': 'Cedula_Extranjeria',
            'Cédula de ciudadanía': 'Cedula_Ciudadania',
            'Cédula de extranjería': 'Cedula_Extranjeria',
            'Cedula_Ciudadania': 'Cedula_Ciudadania',
            'Cedula_Extranjeria': 'Cedula_Extranjeria',
            'NIT': 'NIT',
            'PAS': 'Pasaporte',
            'Secuencial': 'Secuencial',
            'Tarjeta de identidad': 'Tarjeta_Identidad',
            'Tarjeta_Identidad': 'Tarjeta_Identidad'
        }

        for key, value in map_tipo_doc.items():
            cursor.execute("""
                UPDATE Usuario
                SET Tipo_Documento = %s
                WHERE Tipo_Documento = %s;
            """, (value, key))

        # Obtener registros de Numero_Documento
        cursor.execute("SELECT Id, Numero_Documento FROM Usuario;")
        registros = cursor.fetchall()
        total_registros = len(registros)

        for id_usuario, num_doc in registros:
            nuevo_num_doc = re.sub(r'[^0-9]', '', num_doc)  # Eliminar caracteres no numéricos
            
            # Verificar si el número solo tiene ceros
            if nuevo_num_doc == '0' * len(nuevo_num_doc):
                cursor.execute("DELETE FROM Usuario WHERE Id = %s", (id_usuario,))
                registros_eliminados += 1
                continue
            
            nuevo_num_doc = nuevo_num_doc.lstrip('0')  # Eliminar ceros iniciales

            # Verificar si el nuevo número ya existe en la BD usando Numero_Documento_Limpio
            cursor.execute("SELECT COUNT(*) FROM Usuario WHERE Numero_Documento_Limpio = %s", (nuevo_num_doc,))
            duplicado = cursor.fetchone()[0] > 0

            if duplicado:
                cursor.execute("""
                    UPDATE Usuario
                    SET Completitud = 'Validar_Duplicado_Documento'
                    WHERE Id = %s;
                """, (id_usuario,))
                registros_duplicados += 1
            elif num_doc != nuevo_num_doc:
                cursor.execute("""
                    UPDATE Usuario
                    SET Numero_Documento_Limpio = %s, Completitud = 'Validar_Numero_Documento'
                    WHERE Id = %s;
                """, (nuevo_num_doc, id_usuario))
                registros_actualizados += 1
            else:
                registros_sin_cambios += 1
        
        # Limpiar Nombre
        cursor.execute("SELECT Id, Nombre FROM Usuario;")
        nombres = cursor.fetchall()
        
        for id_usuario, nombre in nombres:
            nuevo_nombre = re.sub(r'[^A-Za-zÁÉÍÓÚÑáéíóúñ ]', '', nombre)  # Eliminar caracteres extraños
            nuevo_nombre = re.sub(r'\s+', ' ', nuevo_nombre).strip()  # Un solo espacio entre palabras
            nuevo_nombre = nuevo_nombre.upper()
            
            if nombre != nuevo_nombre:
                cursor.execute("""
                    UPDATE Usuario
                    SET Nombre = %s
                    WHERE Id = %s;
                """, (nuevo_nombre, id_usuario))
                registros_actualizados += 1
        
        conn.commit()
        
        print("Limpieza y estandarización completadas con éxito.")
        print("Resumen de cambios:")
        print(f"Total registros procesados: {total_registros}")
        print(f"Registros actualizados/estandarizados: {registros_actualizados}")
        print(f"Registros eliminados: {registros_eliminados}")
        print(f"Registros con posible duplicidad: {registros_duplicados}")
        print(f"Registros sin cambios: {registros_sin_cambios}")

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    limpiar_y_estandarizar()