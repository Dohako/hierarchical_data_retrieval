import os
import loguru
import psycopg2
import dotenv


dotenv.load_dotenv('./.env')
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")

#establishing the connection
conn = psycopg2.connect(
   database=DATABASE, user=USER, password=PASSWORD, host=HOST, port= PORT
)
conn.autocommit = True
cursor = conn.cursor()
try:
   sql = '''CREATE database mydb''';
   cursor.execute(sql)
   print("Database created successfully........")
except psycopg2.Error:
   loguru.logger.info("Base already exists")

#Closing the connection
conn.close()
# loguru.logger.add('log.log')
#
#
#
# if os.path.isfile('base.json') is False:
#     loguru.logger.info('Добавьте файл с базой')
# else:
#     loguru.logger.info('Добавьте файл с базой')
