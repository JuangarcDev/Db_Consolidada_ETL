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

LOG_FILE = "informe_etl_tucatastro.txt"

def log_message(message):
    with open(LOG_FILE, "a", encoding="utf-8") as log:
        log.write(message + "\n")
    print(message)

def extraer_tramites():
    try:
        log_message("\n[INFO] Extrayendo trámites...")
        conn = psycopg2.connect(**DB_SOURCE)
        cur = conn.cursor()
        cur.execute("SELECT id FROM data.tramite WHERE fecha_radicado >= '2024-01-01' ORDER BY id ASC;")
        tramites = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        log_message(f"[INFO] Se encontraron {len(tramites)} trámites.")
        return tramites
    except Exception as e:
        log_message(f"[ERROR] Error extrayendo trámites: {e}")
        return []

def extraer_interesados(tramites):
    if not tramites:
        return {}, []
    try:
        log_message("\n[INFO] Extrayendo interesados asociados a los trámites...")
        conn = psycopg2.connect(**DB_SOURCE)
        cur = conn.cursor()
        query = """
        SELECT interesado, predio 
        FROM data.relacion_predio_interesado 
        WHERE tramite IN %s ORDER BY id ASC;
        """
        cur.execute(query, (tuple(tramites),))

        interesados_predios = {}
        for interesado, predio in cur.fetchall():
            if interesado and predio:
                if interesado not in interesados_predios:
                    interesados_predios[interesado] = []
                interesados_predios[interesado].append(predio)        
        
        cur.close()
        conn.close()

        solo_interesados = list(interesados_predios.keys())

        log_message(f"[INFO] Se encontraron {len(interesados_predios)} interesados con números prediales.")
        log_message(f"[DEBUG] Muestra de interesados con predios: {list(interesados_predios.items())[:5]}")  # Muestra primeros 5 registros


        log_message(f"[INFO] Se encontraron {len(interesados_predios)} interesados con números prediales.")
        return interesados_predios, solo_interesados
    except Exception as e:
        log_message(f"[ERROR] Error extrayendo interesados y números prediales: {e}")
        return {}, []


def extraer_datos_interesados(interesados):
    if not interesados:
        return []
    try:
        log_message("\n[INFO] Extrayendo datos de los interesados...")
        conn = psycopg2.connect(**DB_SOURCE)
        cur = conn.cursor()
        query = """
        SELECT id_universal, tipo_documento, documento_identidad, primer_nombre, segundo_nombre, 
               primer_apellido, segundo_apellido, fecha_activo 
        FROM data.interesado WHERE id_universal IN %s ORDER BY tipo_documento ASC;
        """
        cur.execute(query, (tuple(interesados),))
        datos = cur.fetchall()
        cur.close()
        conn.close()
        log_message(f"[INFO] Se encontraron {len(datos)} registros de interesados con datos.")
        return datos
    except Exception as e:
        log_message(f"[ERROR] Error extrayendo datos de interesados: {e}")
        return []

def transformar_datos(datos, relacion_predios):
    usuarios = []
    log_message("\n[INFO] Transformando datos...")

    for row in datos:
        try:
            id_universal, tipo_documento, documento_identidad, p_nombre, s_nombre, p_apellido, s_apellido, fecha_registro = row
            if not tipo_documento or not documento_identidad:
                log_message(f"[WARNING] Registro omitido por valores nulos: {row}")
                continue  # Omitir registros con valores nulos

            if len(str(documento_identidad)) > 30:
                log_message(f"[WARNING] Documento de identidad demasiado largo: {documento_identidad}")
                continue  # Omitir registros con documento de identidad inválido

            nombre_completo = " ".join(filter(None, [p_nombre, s_nombre, p_apellido, s_apellido]))

            # Buscar el documento_identidad en el diccionario de relación_predios
            numero_predial = relacion_predios.get(id_universal, None)
            log_message(f"[DEBUG] ID UNIVERSAL: {id_universal}, Número Predial Asociado: {numero_predial}")  # Mensaje de depuración

            # Inicializar valores de Municipio, Zona y Vereda en None
            municipio, zona, vereda = None, None, None

            # Si hay predios, extraer el primero y descomponerlo
            if numero_predial and len(numero_predial) > 0:
                predio_principal = str(numero_predial[0]) # Convertimos a string

                if len(predio_principal) >= 17: # validamos que tenga por lo menos 17 caracteres
                    municipio = predio_principal[:5]
                    zona = predio_principal[5:7]
                    vereda = predio_principal[13:17]

                log_message(f"[DEBUG] Extraído -> Municipio: {municipio}, Zona: {zona}, Vereda: {vereda}")  # Depuración

            # Agregamos datos transformados a la lista de usuarios
            usuarios.append((tipo_documento, documento_identidad, nombre_completo, fecha_registro, municipio, zona, vereda))
        except Exception as e:
            log_message(f"[ERROR] Error transformando datos: {e}")

    log_message(f"[INFO] Se transformaron {len(usuarios)} registros válidos para insertar.")
    return usuarios

def cargar_datos(usuarios):
    if not usuarios:
        log_message("[INFO] No hay datos válidos para insertar en la base de datos destino.")
        return
    try:
        log_message("\n[INFO] Cargando datos en la base de datos destino...")
        conn = psycopg2.connect(**DB_TARGET)
        cur = conn.cursor()
        query = """
        INSERT INTO usuario (tipo_documento, numero_documento, nombre, fecha_registro, fuente, municipio, zona, vereda) 
        VALUES (%s, %s, %s, %s, 'Tu_Catastro_Tramites', %s, %s, %s) 
        ON CONFLICT (numero_documento)
        DO UPDATE SET
            municipio = COALESCE(EXCLUDED.municipio, usuario.municipio),
            zona = COALESCE(EXCLUDED.zona, usuario.zona),
            vereda = COALESCE(EXCLUDED.vereda, usuario.vereda),
            fuente = CASE 
                        WHEN usuario.fuente LIKE '%%Tu_Catastro_Tramites%%' THEN usuario.fuente
                        ELSE usuario.fuente || ', Tu_Catastro_Tramites'
                        END;
        """
        cur.executemany(query, usuarios)
        conn.commit()
        cur.close()
        conn.close()
        log_message(f"[SUCCESS] Se insertaron {len(usuarios)} registros correctamente.")
    except Exception as e:
        log_message(f"[ERROR] Error cargando datos: {e}")

def ejecutar_etl():
    try:
        log_message("\n===== INICIANDO PROCESO ETL =====")
        tramites = extraer_tramites()
        if not tramites:
            log_message("[INFO] No hay trámites para procesar. ETL finalizado.")
            return

        interesados_predios, solo_interesados = extraer_interesados(tramites)
        if not solo_interesados:
            log_message("[INFO] No hay interesados asociados a los trámites. ETL finalizado.")
            return

        datos_interesados = extraer_datos_interesados(solo_interesados)
        if not datos_interesados:
            log_message("[INFO] No se encontraron datos de interesados. ETL finalizado.")
            return

        usuarios = transformar_datos(datos_interesados, interesados_predios)
        if not usuarios:
            log_message("[INFO] No hay usuarios válidos para cargar. ETL finalizado.")
            return

        cargar_datos(usuarios)
        log_message("===== ETL FINALIZADO =====")
    except Exception as e:
        log_message(f"[ERROR] Error general en ETL: {e}")

if __name__ == "__main__":
    ejecutar_etl()