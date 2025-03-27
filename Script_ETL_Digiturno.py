"""
Script con estructura inicial de ETL entre DB consolidada y Aplicativo de Digiturno
"""
import psycopg2
import csv

# Configuración de conexión a PostgreSQL
DB_CONSOLIDADA = {
    "dbname": "Acc_Atencion_Usuarios_Consolidada",
    "user": "postgres",
    "password": "1234jcgg",
    "host": "localhost",
    "port": "5432"
}

# Ruta del archivo CSV
CSV_PATH = r"C:\ACC\Propuesta_Consolidacion_DB\Turnero\clientes.csv"

# Función para validar requisitos mínimos
def es_registro_valido(row):
    """ Verifica que el registro tenga los valores mínimos requeridos """
    return bool(row[2] and row[3])  # numero_documento y nombre son obligatorios

# Función principal de ETL
def etl_csv_to_consolidada():
    try:
        conn = psycopg2.connect(**DB_CONSOLIDADA)
        cursor = conn.cursor()

        # Obtener documentos ya existentes
        cursor.execute("SELECT numero_documento, fuente FROM public.usuario;")
        documentos_existentes = {row[0]: row[1] for row in cursor.fetchall()}

        nuevos_registros = []
        registros_para_actualizar = []

        with open(CSV_PATH, "r", encoding="utf-8") as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                id_, nombre, num_doc, telefono, email, _, _, fecha_registro, _ = row
                
                if not es_registro_valido(row):
                    continue  # Saltar registros sin datos mínimos
                
                tipo_doc = "Cedula_Ciudadania"
                fuente_nueva = "Digiturno"
                telefono = telefono if telefono else None
                email = email if email else None
                fecha_registro = fecha_registro if fecha_registro else None
                
                if num_doc in documentos_existentes:
                    fuente_actual = documentos_existentes[num_doc]
                    nueva_fuente = fuente_actual if "Digiturno" in fuente_actual else fuente_actual + ", Digiturno"
                    registros_para_actualizar.append((nombre, telefono, email, fecha_registro, nueva_fuente, num_doc))
                else:
                    nuevos_registros.append((tipo_doc, num_doc, nombre, telefono, fecha_registro, fuente_nueva, email))
        
        # Insertar nuevos registros
        if nuevos_registros:
            cursor.executemany("""
                INSERT INTO public.usuario (tipo_documento, numero_documento, nombre, telefono, fecha_registro, fuente, email)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, nuevos_registros)

        # Actualizar registros existentes
        if registros_para_actualizar:
            for nombre, telefono, email, fecha_registro, nueva_fuente, num_doc in registros_para_actualizar:
                cursor.execute("""
                    UPDATE usuario
                    SET nombre = COALESCE(usuario.nombre, %s),
                        telefono = COALESCE(usuario.telefono, %s),
                        email = COALESCE(usuario.email, %s),
                        fecha_registro = LEAST(usuario.fecha_registro, %s),
                        fuente = %s
                    WHERE numero_documento = %s;
                """, (nombre, telefono, email, fecha_registro, nueva_fuente, num_doc))

        conn.commit()
        print(f"✅ {len(nuevos_registros)} registros insertados y {len(registros_para_actualizar)} registros actualizados.")
    
    except Exception as e:
        print(f"❌ Error durante la ETL: {e}")
        conn.rollback()
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    etl_csv_to_consolidada()
