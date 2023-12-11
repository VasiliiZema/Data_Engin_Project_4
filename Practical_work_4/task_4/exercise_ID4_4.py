import json
import msgpack
import sqlite3

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
                if row[0] in ("param"):
                    if row[1].strip() == "True":
                        n = 1
                    elif row[1].strip() == "False":
                        n = 0
                    elif row[1].strip() == "":
                        n = 0
                    else:
                        n = row[1].strip()
                    item[row[0]] = float(n)
                else:
                    item[row[0]] = row[1].strip()

    return items

#Создаём функцию для подключения к базе данных SQLite
def connect_to_db(file_db):
    connection = sqlite3.connect(file_db)
    connection.row_factory = sqlite3.Row
    return connection

#Создаём функцию для работы с таблицей к базе данных SQLite
def insert_data(db, data):
    cursor = db.cursor()

    result = cursor.executemany("""
                                    INSERT INTO table_task_4 (name, price, quantity, fromCity, isAvailable, views) 
                                    VALUES (:name, :price, :quantity, :fromCity, :isAvailable, :views)
                                    """, data)

    db.commit()

    return

#Изменения товаров из базы данных (на основе данных об изменение товаров)

def handle_method(cursor, name, method, param=None):
    if method == 'remove':
        cursor.execute('DELETE FROM table_task_4 WHERE name = ?', [name])
    elif method == 'quantity_add':
        cursor.execute('UPDATE table_task_4 SET quantity = quantity + ?, version = version + 1 WHERE name = ?', [abs(param), name])
    elif method == 'quantity_sub':
        cursor.execute('UPDATE table_task_4 SET quantity = quantity - ?, version = version + 1 WHERE name = ? AND ((quantity - ?) > 0)', [abs(param), name, abs(param)])
    elif method == 'price_percent':
        cursor.execute('UPDATE table_task_4 SET price = ROUND(price * (1 + ?), 2), version = version + 1 WHERE name = ?', [param, name])
    elif method == 'price_abs':
        cursor.execute(f"UPDATE table_task_4 SET price = price + ?, version = version + 1 WHERE name = ? AND ((price + ?) > 0)", [param, name, param])
    elif method == 'available':
        cursor.execute('UPDATE table_task_4 SET isAvailable = ?, version = version + 1 WHERE name == ?', [1 if param else 0, name])
    else:
        raise ValueError(f'{method} метода нет!')

def handle_updates(db, data):
    cursor = db.cursor()
    for update in data:
        handle_method(db, update['name'], update['method'], update['param'])
    db.commit()

#Выводим топ-10 самых обновляемых товаров
def top_10(db):
    cursor = db.cursor()
    result = cursor.execute("""SELECT name, price, quantity, fromCity, isAvailable, views, version FROM table_task_4 ORDER BY version DESC LIMIT 10""")
    items = []
    for row in result.fetchall():
        item = dict(row)
        items.append(item)

    db.commit()

    with open("top_10.json", "w") as file_json:
        file_json.write(json.dumps(items, ensure_ascii=False))

    return

#Анализируем цены товаров, находим (сумму, мин, макс, среднее) для каждой группы (fromCity),
#а также количество товаров в группе
def analises_price_products(db):
    cursor = db.cursor()
    result = cursor.execute("""SELECT fromCity,
                                        SUM(price) as sum_price,
                                        MIN(price) as min_price,
                                        MAX(price) as max_price,
                                        AVG(price) as avg_price,
                                        SUM(quantity) as all_quantity
                                FROM table_task_4
                                GROUP BY fromCity
                                ORDER BY all_quantity DESC""")
    items = []
    for row in result.fetchall():
        item = dict(row)
        items.append(item)

    db.commit()

    with open("nalises_price_products.json", "w") as file_json:
        file_json.write(json.dumps(items, ensure_ascii=False))

    return


# Анализируем остатки товаров (quantity), найдя (сумму, мин, макс, среднее) для каждой группы товаров
def analises_quantity_products(db):
    cursor = db.cursor()
    result = cursor.execute("""SELECT fromCity,
                                        SUM(quantity) as sum_quantity,
                                        MIN(quantity) as min_quantity,
                                        MAX(quantity) as max_quantity,
                                        AVG(quantity) as avg_quantity
                                FROM table_task_4
                                GROUP BY fromCity
                                ORDER BY sum_quantity DESC""")
    items = []
    for row in result.fetchall():
        item = dict(row)
        items.append(item)

    db.commit()

    with open("analises_quantity_products.json", "w") as file_json:
        file_json.write(json.dumps(items, ensure_ascii=False))

    return

# Найти товары, количество которых болше 700
def my_analises_products(db):
    cursor = db.cursor()
    result = cursor.execute("""SELECT name, price, quantity, fromCity, isAvailable, views, version
                                FROM table_task_4
                                WHERE quantity > 700
                                ORDER BY quantity DESC""")

    items = []
    for row in result.fetchall():
        item = dict(row)
        items.append(item)

    db.commit()

    with open("my_analises_products.json", "w") as file_json:
        file_json.write(json.dumps(items, ensure_ascii=False))

    return


data = open_msgpack("task_4_var_19_product_data.msgpack")
db = connect_to_db("base_4")
#insert_data(db, data)
change_products = open_txt("task_4_var_19_update_data.text")
#handle_updates(db, change_products)



top_10(db)
analises_price_products(db)
analises_quantity_products(db)
my_analises_products(db)

