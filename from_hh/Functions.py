import random
import time
from datetime import datetime, date
import json
import os
import re
import pandas as pd

# Для удаления тегов
from lxml import html

from DB import get_df_from_db, update_table, create_db_table_from_df
from Download import get_by_url
from append_log import append_log

from random_user_agent.params import SoftwareName, OperatingSystem
from random_user_agent.user_agent import UserAgent

SOFTWARE_NAMES = [SoftwareName.CHROME.value]
OPERATING_SYSTEMS = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
USER_AGENT = UserAgent(software_names=SOFTWARE_NAMES, operating_systems=OPERATING_SYSTEMS, limit=100)

PATH_archive_pagination = r'from_hh/docs/archive/pagination/'
PATH_pagination = r'from_hh/docs/pagination/'
PATH_vacancies = r'from_hh/docs/vacancies/'
PATH_NEW_pagination = r'from_hh/docs/archive/pagination/'
PATH_data = '../from_hh/data/'
URL_areas = 'https://api.hh.ru/areas'


def get_header():
    return {'User-Agent': USER_AGENT.get_random_user_agent()}


# Функция для создания первичной базы вакансий из файлов пагинации
def get_df_from_pagination(path_get_pages):
    """
        На основе данных файлов пагинации формирует полный перечень вакансий с 15 полями для дальнейшей обработки.
    Таблица сводная. Включены данные самой вакансии, работодателя, метаданные.
    path_get_pages: указать расположение файлов пагинации
    ('id_vac', 'name', 'salary_min', 'salary_max', 'salary_currency', 'key_word', 'status_vac', 'created_at', 'url',
    'alternate_url', 'employer_id', 'employer_name', 'employer_url', 'employer_vacancies_url', 'date').
    Создается файл: from_hh/result/hh_employers.csv
    :return:
    """
    columns = ('id', 'name', 'salary_min', 'salary_max', 'salary_currency', 'key_word',
               'status_vac', 'created_at', 'url', 'alternate_url', 'employer_id',
               'employer_name', 'employer_url', 'employer_vacancies_url', 'date')
    file_list = []
    count = 0
    for fl in os.listdir(path_get_pages):
        name_vac = ''.join(re.findall(r'[^\d_.json]', os.path.basename(fl)))
        f = open(f'{path_get_pages}{fl}', encoding='utf8')
        json_text = f.read()
        f.close()
        # Преобразуем полученный текст в объект справочника
        json_obj = json.loads(json_text)
        for v in json_obj['items']:
            # Количество записей и ключи в исходных файлах могут меняться.
            # Для избежания ошибок доступа по ключу или получения неверных данных организовано try
            try:
                lst = [int(v['id']), v['name']]
                # Заполнение по ключам в цикле
                salary = ['from', 'to', 'currency']
                if json_obj['salary'] is not None:
                    for el in salary:
                        try:
                            lst.append(v['salary'][f'{el}'])
                        except KeyError:
                            lst.append('undef')
                lst.append(name_vac)
                lst.append(v['type']['id'])
                lst.append(v['created_at'])
                lst.append(v['url'])
                lst.append(v['alternate_url'])
                # Состав словаря может отличаться. для стандартизации записи устанавливаем формальный критерий.
                # Можно усовершенствовать, если создать универсальное условие, ил пройти в цикле
                if len(v['employer']) > 1:
                    empl_list = ('id', 'name', 'url', 'vacancies_url')
                    for el in empl_list:
                        try:
                            lst.append(v['employer'][el])
                        except KeyError:
                            lst.append('undef')
                else:
                    lst = lst + ['undef', 'undef', 'undef', 'undef']
                lst.append(date.today())
                lst.append(False)
                count += 1
            except KeyError:
                continue
            file_list.append(lst)
        move_file_pagination_to_archive(path_get_pages, fl)
    # Записываем результат в DataFrame
    df = pd.DataFrame(data=file_list, columns=columns)
    # Убираем явные дубликаты, которые могли появиться из-за программной интерпретации ключевых слов
    df = df.drop_duplicates()
    return df


# Функция для переноса отработанного файла
def move_file_pagination_to_archive(path, file_name_str):
    """
    Производится проверка существования директории, одноименной текущей дате. При необходимости создается.
    В эту архивную директорию переносится отработанный файл пагинации.
    :param path: Указывается местоположоние текущего файла
    :param file_name_str: Имя файла
    :return: ничего не возвращает - производит перенос файла в жестко заданную папку архива
    """

    if not os.path.exists(f'{"docs/archive/pagination/"}{str(datetime.now().date())}'):
        os.makedirs(f'{"docs/archive/pagination/"}{str(datetime.now().date())}')
    new_name_file = f'{"docs/archive/pagination/"}{str(datetime.now().date())}/{file_name_str}'
    os.rename(f'{path}{file_name_str}', new_name_file)
    append_log(f'Файл перенесен в архив: {new_name_file}')


# Работа с файлом ключевых слов
def open_key_words_list(mode='r', data=None):
    """
    Функция получает из файла данные перечня ключевых слов для поиска вакансий в строке Name и формирования
    файлов пагинации
    :param mode: 'r' или 'w' определет для чего открывается файл - чтения / записи
    :param data: актуален в режиме "w" принимает для записи пары "Ключевое слово":True/False
    :return: список ключевых слов для обработки парсером - возвращается если режим "r"
    """
    with open(r'../from_hh/data/key_words_list.txt', mode=mode, encoding='utf-8') as f:
        if mode == 'r':
            prof_list = f.readlines()
            if len(prof_list) > 0:
                result = prof_list
            else:
                result = None
            return result
        else:
            f.writelines(data)
    append_log(f'Файл key_words_list.txt обновлен.')


# Получение каждой вакансии по ее url на основе данных таблицы vacancies базы данных vac
def write_json_bu_url_from_db(path=PATH_vacancies):
    """
    Получение каждой вакансии по ее url на основе данных таблицы vacancies базы данных vac
    :param path: место для размещения будущих файлов вакансий в формате json
    :return: ничего не возвращает
    """
    # Получаем DataFrame из базы данных по атрибуту add_file_vac = 0 (Файлы вакансий не получались)
    df = get_df_from_db(select='SELECT id, url, key_word, employer_id, employer_name '
                               'FROM vacancies WHERE add_file_vac = 0;')
    # Список для контроля полученных ваканcий по id и установке отметки о получении в базе данных
    vac_lst = []
    count = 0
    headers = get_header()
    # Попытка получить файл. В случае ошибки сведения о полученных файлах записываются в БД.
    for i, url in enumerate(df['url']):
        if count % 50 == 0:
            time.sleep(random.randint(1, 4))
            # headers = get_header()
        try:
            data = get_by_url(url, headers=headers)
            test = data['id']
            # [id, url, key_word, employer_id, employer_name]
            data['name_vac'] = df.iloc[i, 2]
            data["employer"] = df.iloc[i, 3]
            data['employer_name'] = df.iloc[i, 4]
            file_name = f"{path}{df.iloc[i, 0]}.json"
            data = json.dumps(data, ensure_ascii=False)
            f = open(file_name, mode='w', encoding='utf8')
            f.write(data)
            f.close()
            count += 1
            # Выводим прогресс
            print(f'Файл создан для вакансии: {test}. Всего создано: {count}')
            append_log(f'Файл создан для вакансии: {test}. Всего создано за сессию: {count}')
            vac_lst.append(df.iloc[i, 0])
        # Если получен ошибочный файл, продолжаем
        except KeyError:
            append_log(f'KeyError {df.iloc[i, 0]}')
            print(f'KeyError {df.iloc[i, 0]}')
            # headers = get_header()
            continue
        # Если прекращено принудительно с клавиатуры - KeyboardInterrupt или разрыв связи - выход из цикла.
        except KeyboardInterrupt:
            append_log(f'KeyboardInterrupt {df.iloc[i, 0]}')
            print(f'KeyboardInterrupt {df.iloc[i, 0]}')
            break
        except TimeoutError:
            append_log(f'TimeoutError {df.iloc[i, 0]}')
            print(f'TimeoutError {df.iloc[i, 0]}')
            break
        except ConnectionError:
            append_log(f'ConnectionError {df.iloc[i, 0]}')
            print(f'ConnectionError {df.iloc[i, 0]}')
            break
        if len(vac_lst) > 0:
            update_table(select=f"UPDATE vacancies SET add_file_vac = 1 WHERE "
                                f"id in ({', '.join(str(el) for el in vac_lst)});")


def get_columns_by_table(table):
    """
    Функция на основе контрольной строки DataFrame создает список столбцов таблицы
    :param table: 'Имя таблицы' - string
    :return: tuple(columns)
    """
    columns = get_df_from_db(select=f'SELECT * FROM {table} LIMIT 1').columns
    return tuple(columns)


# КРИВАЯ функция. Переписать
def write_data_to_db_from_vac_files(path=PATH_vacancies, mode='replace'):
    """
    Кривая функция. ПЕРЕПИСАТЬ.
    Обрабатывает все файлы вакансий, разделяя данные на 3 таблицы базы данных.
    :param mode: Установить статус отношения к целевым таблицам базы данных
    """
    # Фиксируем дату обращения
    now = date.today().strftime("%d-%m-%Y")
    # Создаем списки для столбцов таблицы vacancies
    ids = []  # Список идентификаторов вакансий
    names = []  # Список наименований вакансий
    descriptions = []  # Список описаний вакансий
    id_employers = []  # Список с id работодателей
    prof_name = []     # Список наименований профессий (ключевые слова поиска)
    area_id = []    # регион id
    area_name = []    # регион

    spec_vac = []   # Список идентификаторов вакансий
    spec_name_vac = []    # Название вакансии
    spec_id_employers = []  # Код работодателя
    spec_profarea = []      # Прфессиональная сфера
    spec_name = []    # Специализация
    spec_area_id = []    # регион id
    spec_area_name = []    # Регион

    # Создаем списки для столбцов таблицы skills
    skills_vac = []  # Список идентификаторов вакансий
    skills_vac_name = []  # Имена вакансий
    skills_name = []  # Список названий навыков
    skills_prof = []  # Список названий выборки профессий
    skills_area_id = []    # регион id
    skills_area_name = []  # Список названий выборки профессий

    # В выводе будем отображать прогресс
    # Для этого узнаем общее количество файлов, которые надо обработать
    # Счетчик обработанных файлов установим в ноль
    cnt_docs = len(os.listdir(path))
    i = 0

    # Проходимся по всем файлам в папке vacancies
    for fl in os.listdir(path):
        try:
            # Открываем, читаем и закрываем файл
            f = open(f'{path}{fl}', encoding='utf-8')
            json_text = f.read()
            f.close()

            # Текст файла переводим в справочник
            json_obj = json.loads(json_text)
            # Очищаем описание вакансии от тегов
            good_description = html.fromstring(json_obj['description']).text_content()
            # Заполняем списки для таблиц
            ids.append(json_obj['id'])
            names.append(json_obj['name'])
            descriptions.append(good_description)
            id_employers.append(json_obj['employer'])
            prof_name.append(json_obj['name_vac'])
            area_id.append(json_obj['area']['id'])
            area_name.append(json_obj['area']['name'])

            # Т.к. навыки хранятся в виде массива, то проходимся по нему циклом.
            for skl in json_obj['key_skills']:
                # Для обхода ошибки получения данных по ключу при получении имени  и id проверяем равенство списка
                if len([json_obj['id'], json_obj['name_vac'], skl['name']]) == 3:
                    skills_vac.append(json_obj['id'])
                    skills_vac_name.append(json_obj['name'])
                    skills_prof.append(json_obj['name_vac'])
                    # Типизация записей. Приведение к нижнему регистру
                    skill = str(skl['name']).lower()
                    skills_name.append(skill)
                    skills_area_id.append(json_obj['area']['id'])
                    skills_area_name.append(json_obj['area']['name'])
                else:
                    append_log(f'Ошибка заполнения данных для таблицы skills. Файл: {fl}\n')

            for el in json_obj['specializations']:
                spec_vac.append(json_obj['id'])
                spec_name_vac.append(json_obj['name'])
                spec_id_employers.append(json_obj['employer'])
                spec_name.append(el['name'])
                spec_profarea.append(el['profarea_name'])
                spec_area_id.append(json_obj['area']['id'])
                spec_area_name.append(json_obj['area']['name'])

            # Увеличиваем счетчик обработанных файлов на 1, очищаем вывод ячейки и выводим прогресс
            i += 1
        except UnicodeDecodeError:
            print('Ошибка чтения файла')
            append_log(f'Ошибка чтения файла {os.path.basename(fl)}')
            continue
        except KeyError:
            print('KeyError')
            append_log(f'KeyError {os.path.basename(fl)}')

        print(f'Готово {i} из {cnt_docs}')

    # Создаем DataFrame, который затем сохраняем в БД в таблицу vacancies
    df = pd.DataFrame({'id': ids, 'name': names, 'description': descriptions,
                       'id_employers': id_employers, 'prof_name': prof_name, 'area_id': area_id,
                       'area_name': area_name, 'date': now})

    create_db_table_from_df(df, table='descriptions', mode=mode)
    df.to_csv(f'../from_hh/result/hh_vacancies.csv', mode='w')

    # Тоже самое, но для таблицы skills
    df = pd.DataFrame({'id': skills_vac, 'vacancy': skills_vac_name,
                       'skill': skills_name, 'prof': skills_prof, 'area_id': skills_area_id,
                       'area': skills_area_name, 'date': now})

    create_db_table_from_df(df, table='skills', mode=mode)
    df.to_csv(f'../from_hh/result/hh_skills.csv', mode='w')

    # Тоже самое, но для таблицы spec
    df = pd.DataFrame({'vacancy': spec_vac, 'spec_name_vac': spec_name_vac,
                       'id_empl': spec_id_employers, 'skill': spec_name, 'spec_profarea': spec_profarea,
                       'area_id': spec_area_id, 'area': spec_area_name, 'date': now})

    create_db_table_from_df(df, table='specializations', mode=mode)
    df.to_csv(f'../from_hh/result/hh_spec.csv', mode='w')


def remove_captcha():
    """
    Технологическая функция для удаления мусорных записей в папке ../docs/vacancies
    :return: Ничего не возвращает
    """
    ids_lst = []
    for el in os.listdir(PATH_vacancies):
        f = open(f'{PATH_vacancies}{el}', encoding='utf8')
        json_text = f.read()
        f.close()
        # Преобразуем полученный текст в объект справочника
        json_obj = json.loads(json_text)
        try:
            test = json_obj['id']
        except KeyError:
            ids_lst.append(''.join(re.findall(r'\d+', el)))
            os.remove(f'{PATH_vacancies}{el}')
    update_table(select=f"UPDATE vacancies SET add_file_vac=0 WHERE id in ({''.join(ids_lst)});")


def write_areas_json(url=URL_areas, path=PATH_data):
    """
    Функция для получения файла json справочника стран и регионов.
    :param url: url адрес справочника
    :param path: Место сохранения файла справочника с именем areas.json
    :return: Ничего не возвращает
    """
    areas_json = get_by_url(url=url, headers=get_header())
    file_name = f'{path}areas.json'
    data = json.dumps(areas_json, ensure_ascii=False)
    f = open(file_name, mode='w', encoding='utf8')
    f.write(data)
    f.close()


def show_areas(path_file=PATH_data):
    """
    Функция выводит в консоль все дерево справочник areas.json
    :param path_file: место положения файла areas.json
    :return: Ничего не возвращает
    """
    ending = [';  ', '\n']
    f = open(f'{path_file}areas.json', encoding='utf8')
    json_text = f.read()
    f.close()
    # Преобразуем полученный текст в объект справочника
    json_obj = json.loads(json_text)
    # Выводим структуру справочника
    for country in json_obj:
        print(f'{country["id"]}: {country["name"]}')
        for region in country['areas']:
            print(f'\n    {region["id"]}: {region["name"]}')
            print('=' * 20)
            for i, sity in enumerate(region['areas']):
                i_end = (0, 1)[i % 3 == 0]
                tab = (50 - (len(sity["id"]) + len(sity["name"])), 1)[i_end == 1]
                print(f'{sity["id"]}: {sity["name"]}', end=f'{" " * tab}{ending[i_end]}')


def get_areas(path_file=PATH_data, user_area='Россия'):
    """
    Функция для получения одномерного массива типа list  с id городов, входящих в регион или страну
    :param path_file: местоположение файла areas.json
    :param user_area: name или id страны, города, региона: 'Россия'
    :return: list
    """
    result_list = []
    f = open(f'{path_file}areas.json', encoding='utf8')
    json_text = f.read()
    f.close()
    # Преобразуем полученный текст в объект справочника
    json_obj = json.loads(json_text)

    # Рекурсивная функция для обхода всего дерева многомерного массива справочника стран и регионов
    def dim(a):
        """
        Рекурсивная функция для получения списка всех городов, в рамках выбранного региона / страны.
        Итерирует словари и списки
        :param a: Базовый случай рекурсии принимает сложный объект list(dict{: [...]}..)
        :return: Ничего не возвращает - производит заполнение result_list
        """
        if not type(a) == list and len(a['areas']) == 0:
            result_list.append(a['id'])
            # result_list.append(a['name'])
        else:
            if type(a) == list:
                for b in a:
                    dim(b)
            else:
                for b in a['areas']:
                    dim(b)

    # Поиск точки отсчета для позиционирования запроса пользователя и получения всех дочерних записей из справочника
    # Вероятно, можно решить рекурсией, но пока так понятнее.
    for country in json_obj:
        if country['name'] == user_area or country['id'] == user_area:
            dim(country['areas'])
        else:
            for region in country['areas']:
                if region['name'] == user_area or region['id'] == user_area:
                    dim(region['areas'])
                else:
                    for city in region['areas']:
                        if city['name'] == user_area or city['id'] == user_area:
                            result_list.append(city['id'])
                            # result_list.append(city['name'])

    return result_list
