import os
import loguru
import psycopg2
import dotenv
import json

loguru.logger.add('log.log')

if os.path.exists('.env') is True:
    loguru.logger.info("ok")
else:
    loguru.logger.error('add .env file please')
    quit()

dotenv.load_dotenv('./.env')
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")

conn = psycopg2.connect(
    database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT
)
conn.autocommit = True
cursor = conn.cursor()
try:
    sql = '''CREATE database mydb''';
    cursor.execute(sql)
    loguru.logger.info("Database created successfully........")
    if os.path.isfile('base.json') is False:
        loguru.logger.error('Добавьте файл .json с базой')
        quit()
    with open('base.json', 'r', encoding='UTF-8') as base:
        data = json.load(base)
        loguru.logger.info(data[0])
    # TODO make table creation
    sql = '''CREATE table mydb''';
    cursor.execute(sql)
    loguru.logger.info("Database created successfully........")
except psycopg2.Error:
    #  TODO clear deleting
    sql = '''DROP database mydb''';
    cursor.execute(sql)
    loguru.logger.info('Base deleted')
    # loguru.logger.info("Base already exists")
conn.close()