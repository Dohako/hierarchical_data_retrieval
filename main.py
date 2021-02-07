import os
import loguru


def first():
    pass


loguru.logger.add('log.log')

if os.path.isfile('base.json') is False:
    loguru.logger.info('Добавьте файл с базой')
else:
    loguru.logger.info('Добавьте файл с базой')
