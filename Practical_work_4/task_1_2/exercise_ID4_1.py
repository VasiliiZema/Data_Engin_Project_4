import json
import sqlite3


#Функция для парсинга данных из текстового файла
def parse_data(file_text):
    items = []
    with open(file_text, "r", encoding="utf-8") as file:
        line_text = file.readlines()
        item = dict()

        for row in line_text:
            if row == "=====\n":
                items.append(item)
                item = dict()

            else:
                line = row.split("::")

                if line[0] in ("tours_count", "min_rating", "time_on_game"):
                    item[line[0]] = int(line[1])
                elif line[0] == "id":
                    continue
                else:
                    item[line[0]] = line[1].strip()

    return items

#Создаём функцию для подключения к базе данных SQLite
def connect_to_db(file_db):
    connection = sqlite3.connect(file_db)
    connection.row_factory = sqlite3.Row
    return connection

#Создаём функцию для работы с таблицей к базе данных SQLite
def insert_data(db, data):
    cursor = db.cursor()

    cursor.executemany("""
        INSERT INTO table_task_1 (name, city, begin, system, tours_count, min_rating, time_on_game) 
        VALUES(
        :name, :city, :begin, :system, 
        :tours_count, :min_rating, :time_on_game
            )
        """, data)

    db.commit()

#Функция вывода первых (VAR+10) отсортированных по произвольному числовому полю
# строк из таблицы в файл формата json;
VAR = 19
limit = VAR + 10

def filter_data(db, limit):
    cursor = db.cursor()

    result_1 = cursor.execute(f"""SELECT * 
                                    FROM table_task_1 
                                    ORDER BY min_rating DESC 
                                    LIMIT {limit}""")
    items = []

    for row in result_1.fetchall():
        item = dict(row)
        items.append(item)

    db.commit()

    with open("filter_table_task1.json", "w") as file_json:
        file_json.write(json.dumps(items, ensure_ascii=False))


#Функция вывода (сумму, мин, макс, среднее) по числовому полю "tours_count";
def describe_data(db):
    cursor = db.cursor()

    result_2 = cursor.execute("""SELECT SUM(tours_count) AS sum_tours_count,
                                            MIN(tours_count) AS min_tours_count,
                                            MAX(tours_count) AS max_tours_count,
                                            AVG(tours_count) AS avg_tours_count
                                    FROM table_task_1""")

    db.commit()

    with open("describe_data.json", "w") as file_json:
        file_json.write(json.dumps(items, ensure_ascii=False))



#Функция вывода частоты встречаемости для категориального поля "system";
def distinct_count(db):
    cursor = db.cursor()

    result_3 = cursor.execute("""SELECT system, COUNT(city) as count_city
                                    FROM table_task_1
                                    GROUP BY system""")

    db.commit()

    items = []
    for row in result_3.fetchall():
        item = dict(row)
        items.append(item)

    with open("distinct_count_1.json", "w") as file_json:
        file_json.write(json.dumps(items, ensure_ascii=False))


    return


#Функция вывода первых (VAR+10) отфильтрованных по произвольному "system" == "Olympic"
# отсортированных по числовому полю "time_on_game" строк в порядке убывания из таблицы в файл
# формате json.
VAR = 19
limit = VAR + 10

def sorted_filter_data(db, limit):
    cursor = db.cursor()

    result_4 = cursor.execute("""SELECT * 
                                    FROM table_task_1 
                                    WHERE system == "Olympic"
                                    ORDER BY time_on_game DESC 
                                    """)
    items = []

    for row in result_4.fetchall():
        item = dict(row)
        items.append(item)

    db.commit()

    with open("sorted_filter_table_task1.json", "w") as file_json:
        file_json.write(json.dumps(items, ensure_ascii=False))


items = parse_data("task_1_var_19_item.text")

db = connect_to_db('base_1')

#insert_data(db, items)

filter_data(db, limit)
describe_data(db)
distinct_count(db)
sorted_filter_data(db, limit)