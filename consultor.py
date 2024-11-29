import mysql.connector

# Conexión a la base de datos
db = mysql.connector.connect(
    host="localhost",
    user="predictate",
    password="miaucito",
    database="predictate"
)

cursor = db.cursor()

def get_nominations_by_film(film_title):
    try:
        # Consulta para obtener todas las nominaciones asociadas con la película
        query = """
        SELECT 
            a.name AS award_name,
            a.year AS award_year,
            c.name AS category_name,
            p.name AS person_name,
            r.type AS record_type
        FROM records r
        LEFT JOIN films f ON r.film_id = f.id
        LEFT JOIN people p ON r.person_id = p.id
        LEFT JOIN award_categories ac ON r.award_category_id = ac.id
        LEFT JOIN categories c ON ac.category_id = c.id
        LEFT JOIN awards a ON ac.award_id = a.id
        WHERE f.title = %s
        ORDER BY a.year, c.name;
        """
        cursor.execute(query, (film_title,))
        results = cursor.fetchall()

        # Mostrar los resultados
        if results:
            print(f"Nominaciones para la película '{film_title}':\n")
            for row in results:
                award_name, award_year, category_name, person_name, record_type = row
                person = person_name if person_name else "N/A"
                print(f"- {award_name} ({award_year}) - {category_name}")
                print(f"  Persona: {person} | Tipo: {record_type.capitalize()}")
        else:
            print(f"No se encontraron nominaciones para la película '{film_title}'.")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

# Ejemplo de uso
get_nominations_by_film("Anora")

# Cerrar conexión
cursor.close()
db.close()
