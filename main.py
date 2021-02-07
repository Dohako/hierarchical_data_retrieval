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
creating_base:bool = False
database_name: str = 'mydb'
try:
    sql = '''CREATE database mydb'''
    cursor.execute(sql)
    loguru.logger.info("Database created successfully........")
    creating_base = True
except psycopg2.Error as ex:
    loguru.logger.error(ex)
    loguru.logger.info("Base already exists")
conn.close()
conn = psycopg2.connect(
    database=database_name, user=USER, password=PASSWORD, host=HOST, port=PORT
)
conn.autocommit = True
cursor = conn.cursor()
# TODO check if base is empty
creating_base = True

# if base is empty or just created - fill base
if creating_base is True:
    if os.path.isfile('base.json') is False:
        loguru.logger.error('Добавьте файл .json с базой')
        quit()
    with open('base.json', 'r', encoding='UTF-8') as base:
        data = json.load(base)
        loguru.logger.info(len(data[0]))
    # TODO make table creation
    my_keys = [i for i in data[0].keys()]
    sql = f'''
        CREATE TABLE TEST (
        {my_keys[0]} int2, 
        {my_keys[1]} int2, 
        {my_keys[2]} varchar(255), 
        {my_keys[3]} int2
        )
        '''
    cursor.execute(sql)
    try:
        for id, parent_id, name, type in data:
            loguru.logger.info(id)
            sql = f'''
                    INSERT INTO TEST VALUES (
                    {id}, {parent_id}, {name}, {type}
                    )
                '''
            cursor.execute(sql)
    except Exception as ex:
        loguru.logger.error(ex)
