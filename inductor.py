import json
import mysql.connector
import argparse

from dotenv import load_dotenv
import os

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Acceder a las credenciales
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Conectar a la base de datos
import mysql.connector

db = mysql.connector.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
)

cursor = db.cursor()

def insert_smart(award_name, year, record_type, category_name, person_name=None, film_title=None):
    try:
        # Paso 1: Verificar o insertar el premio
        cursor.execute("SELECT id FROM awards WHERE name = %s AND year = %s", (award_name, year))
        award = cursor.fetchone()
        if not award:
            cursor.execute("INSERT INTO awards (name, year) VALUES (%s, %s)", (award_name, year))
            db.commit()
            award_id = cursor.lastrowid
            print(f"Premio creado: {award_name} ({year})")
        else:
            award_id = award[0]
            print(f"Premio existente: {award_name} ({year})")

        # Paso 2: Verificar o insertar la categoría
        cursor.execute("SELECT id FROM categories WHERE name = %s", (category_name,))
        category = cursor.fetchone()
        if not category:
            raise ValueError(f"La categoría '{category_name}' no está predefinida.")
        category_id = category[0]

        # Paso 3: Verificar o insertar la relación premio-categoría
        cursor.execute(
            "SELECT id FROM award_categories WHERE award_id = %s AND category_id = %s",
            (award_id, category_id)
        )
        award_category = cursor.fetchone()
        if not award_category:
            cursor.execute(
                "INSERT INTO award_categories (award_id, category_id) VALUES (%s, %s)",
                (award_id, category_id)
            )
            db.commit()
            award_category_id = cursor.lastrowid
            print(f"Relación premio-categoría creada: {category_name} para {award_name} ({year})")
        else:
            award_category_id = award_category[0]
            print(f"Relación premio-categoría existente: {category_name} para {award_name} ({year})")

        # Paso 4: Verificar o insertar la película
        film_id = None
        if film_title:
            cursor.execute("SELECT id FROM films WHERE title = %s", (film_title,))
            film = cursor.fetchone()
            if not film:
                cursor.execute("INSERT INTO films (title) VALUES (%s)", (film_title,))
                db.commit()
                film_id = cursor.lastrowid
                print(f"Película creada: {film_title}")
            else:
                film_id = film[0]
                print(f"Película existente: {film_title}")

        # Paso 5: Verificar o insertar la persona
        person_id = None
        if person_name:
            cursor.execute("SELECT id FROM people WHERE name = %s", (person_name,))
            person = cursor.fetchone()
            if not person:
                cursor.execute(
                    "INSERT INTO people (name, role, gender) VALUES (%s, %s, %s)",
                    (person_name, "director", "unknown")
                )
                db.commit()
                person_id = cursor.lastrowid
                print(f"Persona creada: {person_name}")
            else:
                person_id = person[0]
                print(f"Persona existente: {person_name}")

        # Paso 6: Verificar si el record ya existe
        cursor.execute(
            """
            SELECT id FROM records
            WHERE award_category_id = %s AND film_id = %s AND person_id = %s AND type = %s
            """,
            (award_category_id, film_id, person_id, record_type)
        )
        existing_record = cursor.fetchone()
        if existing_record:
            print(f"Registro existente: {category_name} para {film_title or person_name}")
            return

        # Paso 7: Insertar el record si no existe
        cursor.execute(
            """
            INSERT INTO records (award_category_id, film_id, person_id, type, source)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (award_category_id, film_id, person_id, record_type, award_name)
        )
        db.commit()
        print(f"Registro creado: {category_name} para {film_title or person_name}")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    except ValueError as ve:
        print(f"Error: {ve}")

def load_json(file_path):
    with open(file_path, "r") as f:
        data = json.load(f)

    award_name = data["award_name"]
    year = data["year"]
    record_type = data["type"]

    for category in data["categories"]:
        category_name = category["name"]

        # Procesar películas
        if "films" in category:
            for film in category["films"]:
                insert_smart(award_name, year, record_type, category_name, film_title=film)

        # Procesar nominados con personas
        if "nominees" in category:
            for nominee in category["nominees"]:
                insert_smart(
                    award_name,
                    year,
                    record_type,
                    category_name,
                    person_name=nominee["person"],
                    film_title=nominee["film"]
                )

# Ejecutar el script con el JSON base

def main():
    # Configurar argparse para manejar argumentos desde la línea de comandos
    parser = argparse.ArgumentParser(description="Cargar datos de premios desde un archivo JSON.")
    parser.add_argument(
        "--file",
        required=True,
        help="Ruta al archivo JSON con los datos de premios."
    )
    args = parser.parse_args()

    # Cargar y procesar el archivo JSON proporcionado
    json_file = args.file
    print(f"Procesando archivo: {json_file}")
    load_json(json_file)

if __name__ == "__main__":
    main()

    # Cerrar conexión a la base de datos
    cursor.close()
    db.close()


# Cerrar conexión
cursor.close()
db.close()
