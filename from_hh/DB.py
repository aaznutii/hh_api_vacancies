# coding=utf8

from append_log import append_log
import mysql.connector
from sqlalchemy import create_engine, MetaData, Table, Column, Integer
import pandas as pd

FILE_NAME = "from_hh/result/hh_employers.csv"
LOGIN = 'user'
PASSW = input('Enter your password:\n')
DB_NAME = 'vac'

"""
Код для импорта из таблицы в БД
"""


def create_db_table_from_df(DataFrame, login=LOGIN, password=PASSW, db_name=DB_NAME,
                            table='vacancies', mode='replace'):
    """
    Записывает данные из DataFrame в mysql db
    :param DataFrame: Принимается объект Pandas DataFrame
    :param login: строка - логин доступа к БД
    :param password: строка - пароль доступа к БД
    :param db_name: строка - имя БД
    :param table: строка - имя таблицы для записи
    :param mode: строка - способ записи 'replace' - с заменой таблицы целиком; 'append' - дозапись
    """

    # Создать соединение и указать кодировку
    engine = create_engine(f'mysql://{login}:{password}@localhost/{db_name}?charset=utf8', echo=True)
    # Если таблица не существует, то будет создана
    meta = MetaData(engine)
    Table(table, meta, Column('id', Integer, primary_key=True))
    meta.create_all()
    # Записать датафрейм в базу поверх данных с заменой (replace). Можно писать в конец имеющейся таблицы ('append')
    # index=False - признак того, что индекс не участвует в качестве данных.
    DataFrame.to_sql(table, con=engine, if_exists=mode, index=False)
    append_log(f'Произведена запись данных из таблицы DataFrame пользователем : {login};'
               f'Запись произведена в table: {table}, db_name: {db_name}; '
               f'Тип записи {mode}.')
    """ПС. replace очищает всю таблицу вместе с форматами данных (Нужно удалять ключи)."""


def create_db_table_from_csv(file_name=FILE_NAME, login=LOGIN, password=PASSW, db_name=DB_NAME,
                       table='vacancies', mode='replace'):
    """
    Записывает данные из csv файла в mysql db
    :param file_name: строка - файл источник
    :param login: строка - логин доступа к БД
    :param password: строка - пароль доступа к БД
    :param db_name: строка - имя БД
    :param table: строка - имя таблицы для записи
    :param mode: строка - способ записи 'replace' - с заменой таблицы целиком; 'append' - дозапись
    """
    # Получить данные файла
    df = pd.read_csv(file_name, low_memory=False)
    # Убираем явные дубликаты, которые могли появиться из-за программной интерпретации ключевых слов
    df = df.drop_duplicates()
    # Создать соединение и указать кодировку
    engine = create_engine(f'mysql://{login}:{password}@localhost/{db_name}?charset=utf8', echo=True)
    # Записать датафрейм в базу поверх данных с заменой (replace). Можно писать в конец имеющейся таблицы ('append')
    # index=False - признак того, что индекс не участвует в качестве данных.
    df.to_sql(table, con=engine, if_exists=mode, index=False)
    append_log(f'Произведена запись данных пользователем: {login};'
               f'Источник данных: {file_name};'
               f'Запись произведена в table: {table}, db_name: {db_name}; '
               f'Тип записи {mode}.')
    """ПС. replace очищает всю таблицу вместе с форматами данных (Нужно удалять ключи)."""


# Получение данных из базы данных по запросу в датафрейм пандас
def get_df_from_db(login=LOGIN, password=PASSW, db_name=DB_NAME, select='SELECT COUNT(id) FROM vacancies;'):
    """
    Получение данных из базы данных по запросу в датафрейм пандас
    :param login: строка - логин доступа к БД
    :param password: строка - пароль доступа к БД
    :param db_name: строка - имя БД
    :param select: запрос mysql в виде строки 'SELECT COUNT(id_vac) FROM vacancies;'
    :return: DataFrame
    """
    con = mysql.connector.connect(user=login, passwd=password, database=db_name)
    sql = pd.read_sql(select, con)
    con.close()
    return sql


def update_table(login=LOGIN, password=PASSW, db_name=DB_NAME, select=f'UPDATE vacancies SET add_file_vac=1;'):
    con = mysql.connector.connect(user=login, passwd=password, database=db_name)
    con.cursor().execute(select)
    con.commit()

