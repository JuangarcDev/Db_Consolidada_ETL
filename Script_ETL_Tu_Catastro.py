import psycopg2

# Parámetros de conexión
DB_SOURCE = {
    "dbname": "Tu_Catastro_Backup_26032025",
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

def extraer_tramites():
    try:
        print("\n[INFO] Extrayendo trámites...")
        conn = psycopg2.connect(**DB_SOURCE)
        cur = conn.cursor()
        cur.execute("SELECT id FROM data.tramite WHERE fecha_radicado >= '2024-01-01' ORDER BY id ASC;")
        tramites = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        print(f"[INFO] Se encontraron {len(tramites)} trámites.")
        return tramites
    except Exception as e:
        print(f"[ERROR] Error extrayendo trámites: {e}")
        return []

def extraer_interesados(tramites):
    if not tramites:
        return []
    try:
        print("\n[INFO] Extrayendo interesados asociados a los trámites...")
        conn = psycopg2.connect(**DB_SOURCE)
        cur = conn.cursor()
        query = "SELECT interesado FROM data.relacion_predio_interesado WHERE tramite IN %s ORDER BY id ASC;"
        cur.execute(query, (tuple(tramites),))
        interesados = [row[0] for row in cur.fetchall() if row[0]]
        cur.close()
        conn.close()
        print(f"[INFO] Se encontraron {len(interesados)} interesados.")
        return interesados
    except Exception as e:
        print(f"[ERROR] Error extrayendo interesados: {e}")
        return []

def extraer_datos_interesados(interesados):
    if not interesados:
        return []
    try:
        print("\n[INFO] Extrayendo datos de los interesados...")
        conn = psycopg2.connect(**DB_SOURCE)
        cur = conn.cursor()
        query = """
        SELECT tipo_documento, documento_identidad, primer_nombre, segundo_nombre, 
               primer_apellido, segundo_apellido, fecha_activo 
        FROM data.interesado WHERE id_universal IN %s ORDER BY tipo_documento ASC;
        """
        cur.execute(query, (tuple(interesados),))
        datos = cur.fetchall()
        cur.close()
        conn.close()
        print(f"[INFO] Se encontraron {len(datos)} registros de interesados con datos.")
        return datos
    except Exception as e:
        print(f"[ERROR] Error extrayendo datos de interesados: {e}")
        return []

def transformar_datos(datos):
    usuarios = []
    print("\n[INFO] Transformando datos...")
    for row in datos:
        try:
            tipo_documento, documento_identidad, p_nombre, s_nombre, p_apellido, s_apellido, fecha_registro = row
            if not tipo_documento or not documento_identidad:
                print(f"[WARNING] Registro omitido por valores nulos: {row}")
                continue  # Omitir registros con valores nulos

            if len(str(documento_identidad)) > 30:
                print(f"[WARNING] Documento de identidad demasiado largo: {documento_identidad}")
                continue  # Omitir registros con documento de identidad inválido

            nombre_completo = " ".join(filter(None, [p_nombre, s_nombre, p_apellido, s_apellido]))
            usuarios.append((tipo_documento, documento_identidad, nombre_completo, fecha_registro))
        except Exception as e:
            print(f"[ERROR] Error transformando datos: {e}")

    print(f"[INFO] Se transformaron {len(usuarios)} registros válidos para insertar.")
    return usuarios

def cargar_datos(usuarios):
    if not usuarios:
        print("[INFO] No hay datos válidos para insertar en la base de datos destino.")
        return
    try:
        print("\n[INFO] Cargando datos en la base de datos destino...")
        conn = psycopg2.connect(**DB_TARGET)
        cur = conn.cursor()
        query = """
        INSERT INTO usuario (tipo_documento, numero_documento, nombre, fecha_registro, fuente) 
        VALUES (%s, %s, %s, %s, 'Tu_Catastro_Tramites') 
        ON CONFLICT (numero_documento) DO NOTHING;
        """
        cur.executemany(query, usuarios)
        conn.commit()
        cur.close()
        conn.close()
        print(f"[SUCCESS] Se insertaron {len(usuarios)} registros correctamente.")
    except Exception as e:
        print(f"[ERROR] Error cargando datos: {e}")

def ejecutar_etl():
    try:
        print("\n===== INICIANDO PROCESO ETL =====")
        tramites = extraer_tramites()
        if not tramites:
            print("[INFO] No hay trámites para procesar. ETL finalizado.")
            return

        interesados = extraer_interesados(tramites)
        if not interesados:
            print("[INFO] No hay interesados asociados a los trámites. ETL finalizado.")
            return

        datos_interesados = extraer_datos_interesados(interesados)
        if not datos_interesados:
            print("[INFO] No se encontraron datos de interesados. ETL finalizado.")
            return

        usuarios = transformar_datos(datos_interesados)
        if not usuarios:
            print("[INFO] No hay usuarios válidos para cargar. ETL finalizado.")
            return

        cargar_datos(usuarios)
        print("===== ETL FINALIZADO =====")
    except Exception as e:
        print(f"[ERROR] Error general en ETL: {e}")

if __name__ == "__main__":
    ejecutar_etl()