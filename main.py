import os
import loguru
import psycopg2
import dotenv
import json

loguru.logger.add('log.log')

if os.path.exists('.env') is False:
    loguru.logger.error('add .env file please')
    quit()

dotenv.load_dotenv('./.env')
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")

conn = psycopg2.connect(
    database='postgres', user=USER, password=PASSWORD, host=HOST, port=PORT
)
conn.autocommit = True
cursor = conn.cursor()
creation_trigger: bool = False
try:
    sql = f'''CREATE database {DATABASE}'''
    cursor.execute(sql)
    print('Создание базы данных прошло успешно')
    creation_trigger = True
except psycopg2.Error:
    answer = str(input('База уже существует, желаете продолжить?\n'))
    negative = ['н', 'нет', 'n', 'no']
    if answer.lower() in negative:
        print('Всего хорошего')
        quit()

conn.close()
conn = psycopg2.connect(
    database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT
)
conn.autocommit = True
cursor = conn.cursor()

try:
    sql = '''CREATE TABLE TEST()'''
    cursor.execute(sql)
    sql = '''DROP TABLE TEST()'''
    cursor.execute(sql)
    creation_trigger = True
except psycopg2.Error:
    answer = str(input('Таблица внутри базы уже существует, желаете продолжить?\n'))
    negative = ['н', 'нет', 'n', 'no']
    if answer.lower() in negative:
        print('Всего хорошего')
        quit()
if creation_trigger is True:
    if os.path.isfile('base.json') is False:
        loguru.logger.error('Добавьте файл .json с базой')
        quit()
    with open('base.json', 'r', encoding='UTF-8') as base:
        data = json.load(base)
    my_keys = [i for i in data[0].keys()]
    sql = f'''
        CREATE TABLE TEST (
        {my_keys[0]} int2 PRIMARY KEY, 
        {my_keys[1]} int2, 
        {my_keys[2]} varchar(255), 
        {my_keys[3]} int2
        )
        '''
    cursor.execute(sql)

    # filling data
    for d in data:
        if d[my_keys[1]] is None:
            d[my_keys[1]] = 'DEFAULT'
        sql = f'''
                INSERT INTO TEST ({my_keys[0]}, {my_keys[1]}, {my_keys[2]}, {my_keys[3]}) VALUES (
                {d[my_keys[0]]}, {d[my_keys[1]]}, '{d[my_keys[2]]}', {d[my_keys[3]]}
                )
            '''
        cursor.execute(sql)

# try:
#     if os.path.isfile('base.json') is False:
#         loguru.logger.error('Добавьте файл .json с базой')
#         quit()
#     with open('base.json', 'r', encoding='UTF-8') as base:
#         data = json.load(base)
#     my_keys = [i for i in data[0].keys()]
#     sql = f'''
#         CREATE TABLE TEST (
#         {my_keys[0]} int2 PRIMARY KEY,
#         {my_keys[1]} int2,
#         {my_keys[2]} varchar(255),
#         {my_keys[3]} int2
#         )
#         '''
#     cursor.execute(sql)
# except psycopg2.Error as ex:
#     loguru.logger.info(ex)
#     loguru.logger.info("Table already exists")

# try:
#     for d in data:
#         if d[my_keys[1]] is None:
#             d[my_keys[1]] = 'DEFAULT'
#         sql = f'''
#                 INSERT INTO TEST ({my_keys[0]}, {my_keys[1]}, {my_keys[2]}, {my_keys[3]}) VALUES (
#                 {d[my_keys[0]]}, {d[my_keys[1]]}, '{d[my_keys[2]]}', {d[my_keys[3]]}
#                 )
#             '''
#         cursor.execute(sql)
# except Exception as ex:
#     loguru.logger.error(ex)
#     loguru.logger.info("base is full")

print('Начало работы иерархического сбора данных')
while True:
    input_id = input('Введите идентификатор:\n')
    if input_id.isdecimal() is False:
        print('Пожалуйста, введите id в форме числа\n')
        continue
    sql = f'''SELECT * FROM TEST WHERE id = {int(input_id)}'''
    cursor.execute(sql)
    info_from_base = cursor.fetchone()
    if info_from_base[1] is None:
        print(info_from_base[2])
        continue
    sql = f'''SELECT * FROM TEST WHERE id = {info_from_base[1]}'''
    cursor.execute(sql)
    second_info = cursor.fetchone()
    if second_info[1] is None:
        print(f'{info_from_base[2]} в {second_info[2]}')
        continue
    sql = f'''SELECT * FROM TEST WHERE id = {second_info[1]}'''
    cursor.execute(sql)
    third_info = cursor.fetchone()
    if third_info[1] is None:
        if info_from_base[3] != 3:
            print(f'{info_from_base[2]} принадлежащий {second_info[2]} находящийся в{third_info[2]}')
            continue
        sql = f'''SELECT id FROM TEST WHERE parentid = {third_info[0]}'''
        cursor.execute(sql)
        departments = cursor.fetchall()
        result = f'{third_info[2]}: '
        values = []
        for department in departments:
            sql = f'''SELECT id,name,type FROM TEST WHERE parentid = {department[0]}'''
            cursor.execute(sql)
            for value in cursor.fetchall():
                if value[2] == 3:
                    if value[1] not in values:
                        values.append(value[1])
                elif value[2] == 2:
                    sql = f'''SELECT name FROM TEST WHERE parentid = {value[0]}'''
                    cursor.execute(sql)
                    for v in cursor.fetchall():
                        if v[0] not in values:
                            values.append(v[0])
        for v in values:
            result += f'{v}, '
        result = result[:len(result) - 2]
        print(result)
        continue
    sql = f'''SELECT * FROM TEST WHERE id = {third_info[1]}'''
    cursor.execute(sql)
    fourth_info = cursor.fetchone()
    if fourth_info[1] is not None:
        print(f'Проблема с id {info_from_base[0]} из-за того, что id {fourth_info[0]}'
              f'имеет parentid {fourth_info[1]}')
        continue
    sql = f'''SELECT id FROM TEST WHERE parentid = {fourth_info[0]}'''
    cursor.execute(sql)
    departments = cursor.fetchall()
    result = f'{fourth_info[2]}: '
    values = []
    for department in departments:
        sql = f'''SELECT id, name, type FROM TEST WHERE parentid = {department[0]}'''
        cursor.execute(sql)
        for value in cursor.fetchall():
            if value[2] == 3:
                if value[1] not in values:
                    values.append(value[1])
            elif value[2] == 2:
                sql = f'''SELECT name FROM TEST WHERE parentid = {value[0]}'''
                cursor.execute(sql)
                for v in cursor.fetchall():
                    if v[0] not in values:
                        values.append(v[0])
        # value = [value[0] for value in cursor.fetchall()]
    for v in values:
        result += f'{v}, '
    result = result[:len(result) - 2]
    print(result)
conn.close()
