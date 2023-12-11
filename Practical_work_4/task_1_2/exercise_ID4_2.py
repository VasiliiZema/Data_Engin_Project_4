import json
import sqlite3


#Функция считывания данных с json file
def load_data(file_json):
    with open(file_json, "r", encoding="utf-8") as file:
        json_list = json.load(file)

    return json_list

#Функция для подключения к базе данных
def connect_to_db(file_db):
    connection = sqlite3.connect(file_db)
    connection.row_factory = sqlite3.Row

    return connection

#Создаём функцию для работы с таблицей к базе данных SQLite
def insert_data(db, data):
    cursor = db.cursor()

    cursor.executemany("""
        INSERT INTO table_task_2 (table_task_1_id, name, place, prise)
        VALUES(
        (SELECT id FROM table_task_1 WHERE name = :name), :name, :place, :prise)
        """, data)

    db.commit()

#Выбираем из таблицы table_task_2 турнир, которые есть в таблице table_task_1 - поле(name) ,
# сортируем его по призовым - поле(prise) и записываем полученное в json file (запрос result_1);
#В запросе result_2 расчитываем средний, максимальный, минимальный размер призовых поле(prise), в выбранном турнире и выводим его.

def fist_query(db, name):
    cursor = db.cursor()

    result_1 = cursor.execute("""
                                    SELECT * 
                                    FROM table_task_2 
                                    WHERE name = (SELECT name FROM table_task_1 WHERE name = ?)
                                    ORDER BY prise DESC
                                    """, [name])
    items = []

    for row in result_1.fetchall():
        item = dict(row)
        #print(item)
        items.append(item)

    with open("filter_table_task2.json", "w") as file_json:
        file_json.write(json.dumps(items, ensure_ascii=False))

    result_2 = cursor.execute("""
                                    SELECT AVG(prise) as avg_prise, MAX(prise) as max_prise, MIN(prise) as min_prise 
                                    FROM table_task_2
                                    WHERE name = (SELECT name FROM table_task_1 WHERE name = ?) 
                                                                        """, [name])
    items = []
    item = dict(result_2.fetchone())
    items.append(item)

    with open("describe_price.json", "w") as file_json:
        file_json.write(json.dumps(items, ensure_ascii=False))

    cursor.close()
    return



items = load_data("task_2_var_19_subitem.json")
db = connect_to_db("base_1")
fist_query(db, "Макартур 1960")
#insert_data(db, items)
