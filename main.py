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
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")

conn = psycopg2.connect(
    database='postgres', user=USER, password=PASSWORD, host=HOST, port=PORT
)
conn.autocommit = True
cursor = conn.cursor()
creating_base: bool = False
database_name: str = 'mydb'
try:
    sql = '''CREATE database mydb'''
    cursor.execute(sql)
    loguru.logger.info("Database created successfully........")
    creating_base = True
except psycopg2.Error as ex:
    loguru.logger.error(ex)
    loguru.logger.info("Base already exists")
    print('База уже существует, желаете продолжить?\n')
conn.close()
conn = psycopg2.connect(
    database=database_name, user=USER, password=PASSWORD, host=HOST, port=PORT
)
conn.autocommit = True
cursor = conn.cursor()
# TODO check if base is empty
try:
    if os.path.isfile('base.json') is False:
        loguru.logger.error('Добавьте файл .json с базой')
        quit()
    with open('base.json', 'r', encoding='UTF-8') as base:
        data = json.load(base)
        loguru.logger.info(len(data[0]))
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
except psycopg2.Error as ex:
    loguru.logger.info(ex)
    loguru.logger.info("Table already exists")

try:
    for d in data:
        if d[my_keys[1]] is None:
            d[my_keys[1]] = 'DEFAULT'
        sql = f'''
                INSERT INTO TEST ({my_keys[0]}, {my_keys[1]}, {my_keys[2]}, {my_keys[3]}) VALUES (
                {d[my_keys[0]]}, {d[my_keys[1]]}, '{d[my_keys[2]]}', {d[my_keys[3]]}
                )
            '''
        cursor.execute(sql)
except Exception as ex:
    loguru.logger.error(ex)
    loguru.logger.info("base is full")

loguru.logger.info('Start process')
while True:
    input_id = input('Введите идентификатор\n')
    if input_id.isdecimal() is False:
        loguru.logger.info('Wrong input')
        continue
    sql = f'''SELECT * FROM TEST WHERE id = {int(input_id)}'''
    cursor.execute(sql)
    info_from_base = cursor.fetchone()
    loguru.logger.info(info_from_base)
    if info_from_base[1] is None:
        loguru.logger.info(info_from_base[2])
        continue
    sql = f'''SELECT * FROM TEST WHERE id = {info_from_base[1]}'''
    cursor.execute(sql)
    second_info = cursor.fetchone()
    loguru.logger.info(second_info)
    if second_info[1] is None:
        loguru.logger.info(f'{info_from_base[2]} в {second_info[2]}')
        continue
    sql = f'''SELECT * FROM TEST WHERE id = {second_info[1]}'''
    cursor.execute(sql)
    third_info = cursor.fetchone()
    loguru.logger.info(third_info)
    if third_info[1] is None:
        loguru.logger.info(f'сотрудник {info_from_base[2]} в {second_info[2]} из {third_info[2]}')
        if info_from_base[3] != 3:
            loguru.logger.info(f'{info_from_base[2]} принадлежащий {second_info[2]} находящийся в{third_info[2]}')
            continue
        sql = f'''SELECT id FROM TEST WHERE parentid = {third_info[0]}'''
        cursor.execute(sql)
        departments = cursor.fetchall()
        loguru.logger.info(departments)
        result = f'{third_info[2]}: '
        for department in departments:
            sql = f'''SELECT name FROM TEST WHERE parentid = {department[0]}'''
            cursor.execute(sql)
            value = [value[0] for value in cursor.fetchall()]
            for v in value:
                result += f'{v}, '
        result = result[:len(result) - 2]
        loguru.logger.info(result)
        continue
    sql = f'''SELECT * FROM TEST WHERE id = {third_info[1]}'''
    cursor.execute(sql)
    fourth_info = cursor.fetchone()
    loguru.logger.info(fourth_info)
    if fourth_info[1] is not None:
        loguru.logger.error(f'something wrong with id {info_from_base[0]} because id {fourth_info[0]}'
                            f'have parentid {fourth_info[1]}')
        continue
    sql = f'''SELECT id FROM TEST WHERE parentid = {fourth_info[0]}'''
    cursor.execute(sql)
    departments = cursor.fetchall()
    loguru.logger.info(departments)
    result = f'{fourth_info[2]}: '
    for department in departments:
        sql = f'''SELECT id, name, type FROM TEST WHERE parentid = {department[0]}'''
        cursor.execute(sql)
        values = []
        for value in cursor.fetchall():
            if value[2] == 3:
                values.append(value[1])
            elif value[2] == 2:
                sql = f'''SELECT name FROM TEST WHERE parentid = {value[0]}'''
                cursor.execute(sql)
                for v in cursor.fetchall():
                    values.append(v[0])
        # value = [value[0] for value in cursor.fetchall()]
        for v in values:
            result += f'{v}, '
    result = result[:len(result) - 2]
    loguru.logger.info(result)
conn.close()
