import os
import loguru
import psycopg2
import dotenv
import json


def get_result_data(last_dep, cursor: any):
    '''
    Function to dig for two layers in base and get names of employees
    :param last_dep: [id, parentid, name, type] list with ladt department info to dig by it parentId and so on
    :param cursor: cursor object of psycopg (shadowing is ok)
    :return: list with names of employees
    '''
    sql = f'''SELECT id FROM TEST WHERE parentid = {last_dep[0]}'''
    cursor.execute(sql)
    departments = cursor.fetchall()
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
    return values


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
negative = ['н', 'нет', 'n', 'no','esc','quit','quit()','exit']

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
    if answer.lower() in negative:
        print('Всего хорошего')
        quit()

conn.close()
conn = psycopg2.connect(
    database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT
)
conn.autocommit = True
cursor = conn.cursor()

# check if there already is a table in base
try:
    sql = '''CREATE TABLE TEST()'''
    cursor.execute(sql)
    sql = '''DROP TABLE TEST()'''
    cursor.execute(sql)
    creation_trigger = True
except psycopg2.Error:
    answer = str(input('Таблица внутри базы уже существует, желаете продолжить?\n'))
    if answer.lower() in negative:
        print('Всего хорошего')
        quit()
if creation_trigger is True:
    # check if data source is available
    if os.path.isfile('base.json') is False:
        loguru.logger.error('Добавьте файл .json с базой')
        quit()
    with open('base.json', 'r', encoding='UTF-8') as base:
        data = json.load(base)
    my_keys = [i for i in data[0].keys()]
    # creating table
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

print('Начало работы иерархического сбора данных')
while True:
    input_id = str(input('Введите идентификатор:\n'))
    if input_id.lower() in negative:
        print('Всего хорошего\n')
        quit()
    elif input_id.isdecimal() is False:
        print('Пожалуйста, введите id в форме числа')
        continue
    sql = f'''SELECT * FROM TEST WHERE id = {int(input_id)}'''
    cursor.execute(sql)
    first_level = cursor.fetchone()
    if first_level[1] is None:
        print(first_level[2])
        continue
    sql = f'''SELECT * FROM TEST WHERE id = {first_level[1]}'''
    cursor.execute(sql)
    second_level = cursor.fetchone()
    if second_level[1] is None:
        print(f'{first_level[2]} в {second_level[2]}')
        continue
    sql = f'''SELECT * FROM TEST WHERE id = {second_level[1]}'''
    cursor.execute(sql)
    third_level = cursor.fetchone()
    if third_level[1] is None:
        if first_level[3] != 3:
            print(f'{first_level[2]} принадлежащий {second_level[2]} находящийся в{third_level[2]}')
            continue
        data = get_result_data(third_level,cursor)
        result = f'{third_level[2]}: '
        for value in data:
            result += f'{value}, '
        result = result[:len(result) - 2]
        print(result)
        continue
    sql = f'''SELECT * FROM TEST WHERE id = {third_level[1]}'''
    cursor.execute(sql)
    fourth_level = cursor.fetchone()
    if fourth_level[1] is not None:
        print(f'Проблема с id {first_level[0]} из-за того, что id {fourth_level[0]}'
              f'имеет parentid {fourth_level[1]}')
        continue
    data = get_result_data(fourth_level, cursor)
    result = f'{fourth_level[2]}: '
    for value in data:
        result += f'{value}, '
    result = result[:len(result) - 2]
    print(result)
conn.close()
