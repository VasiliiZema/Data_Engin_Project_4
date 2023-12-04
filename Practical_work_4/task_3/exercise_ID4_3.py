import json
import msgpack
import  sqlite3

#Функция для распаковки msg_file
def open_msgpack(file_msg):
    with open(file_msg, 'rb') as file:
        res = msgpack.load(file)

    return  res


#Функция для распаковки txt_file и его обработки
def open_txt(file_txt):
    with open(file_txt, 'r', encoding="utf-8") as file:
        res = file.readlines()

        items = []
        item = dict()
        for elem in res:
            if elem == "=====\n":
                items.append(item)
                item = dict()
            else:
                if elem == "\n":
                    break

                row = elem.split("::")
                if row[0] in ("duration_ms", "year"):
                    item[row[0]] = int(row[1].strip())
                elif row[0] in ("tempo", "instrumentalness"):
                    item[row[0]] = float(row[1].strip())
                else:
                    item[row[0]] = row[1].strip()

    return items


#Создаём функцию для подключения к базе данных SQLite
def connect_to_db(file_db):
    return sqlite3.connect(file_db)

#Создаём функцию для работы с таблицей к базе данных SQLite
def insert_data(db, data):
    cursor = db.cursor()

    cursor.executemany("""
        INSERT INTO table_task_3 (artist, song, duration_ms, year, tempo, 
        genre, instrumentalness) 
        VALUES(
        :artist, :song, :duration_ms, :year, 
        :tempo, :genre, :instrumentalness
            )
        """, data)

    db.commit()

#Функция вывода первых (VAR+10) отсортированных по числовому полю "year"
#строк из таблицы в файл формата json;
VAR = 19
limit_1 = VAR + 10

def sorted_data(db, limit):
    cursor = db.cursor()

    result_1 = cursor.execute("""SELECT * 
                                FROM table_task_3 
                                ORDER BY year DESC 
                                LIMIT ?""", [limit])

    items = []
    for row in result_1:
        item = dict()
        item["artist"] = row[1]
        item["song"] = row[2]
        item["duration_ms"] = row[3]
        item["year"] = row[4]
        item["tempo"] = row[5]
        item["genre"] = row[6]
        item["instrumentalness"] = row[7]
        items.append(item)

    cursor.close()

    with open("sorted_table_task3.json", "w") as file_json:
        file_json.write(json.dumps(items, ensure_ascii=False))

    return

#Функция вывода (сумму, мин, макс, среднее) по числовому полю "duration_ms";
def describe_data(db):
    cursor = db.cursor()

    result_2 = cursor.execute("""SELECT SUM(duration_ms) AS sum_duration_ms,
                                            MIN(duration_ms) AS min_duration_ms,
                                            MAX(duration_ms) AS max_duration_ms,
                                            AVG(duration_ms) AS avg_duration_ms
                                    FROM table_task_3""")

    print("Вывод (суммы, мин, макс, среднего) по числовому полю 'duration_ms'")
    describe = result_2.fetchall()[0]
    item = dict({ "sum_duration_ms": describe[0],
                  "min_duration_ms": describe[1],
                  "max_duration_ms": describe[2],
                  "avg_duration_ms": describe[3]
                })

    print(item)

    cursor.close()

    return

#Функция вывода частоты встречаемости для категориального поля "artist",
#сортировки по количеству треков одного исполнителя и записи данных в json_file;
limit_2 = VAR + 15
def count_caregori_data(db, limit):
    cursor = db.cursor()

    result_3 = cursor.execute("""SELECT artist, COUNT(year) as count_artist
                                    FROM table_task_3
                                    GROUP BY  artist
                                    ORDER BY 2 DESC
                                    LIMIT ?""", [limit])

    items = [dict(result_3.fetchall())]
    print(items)

    with open("filter_table_task3.json", "w", encoding="utf-8") as file_json:
        file_json.write(json.dumps(items, ensure_ascii=False))

    cursor.close()
    return


result = open_msgpack("task_3_var_19_part_2.msgpack") + open_txt("task_3_var_19_part_1.text")
db = connect_to_db("base_3")
#insert_data(db, result)
sorted_data(db, limit_1)
describe_data(db)
count_caregori_data(db, limit_2)

