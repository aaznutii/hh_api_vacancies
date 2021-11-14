
# Библиотека для работы с HTTP-запросами. Будем использовать ее для обращения к API HH
import random
import re

import requests
# Пакет для удобной работы с данными в формате json
import json
# Модуль для работы со значением времени
import time
# Модуль для работы с операционной системой. Будем использовать для работы с файлами
import os
# Библиотека для изменения метаданных запроса
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
from append_log import append_log


SOFTWARE_NAMES = [SoftwareName.CHROME.value]
OPERATING_SYSTEMS = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
USER_AGENT = UserAgent(software_names=SOFTWARE_NAMES, operating_systems=OPERATING_SYSTEMS, limit=100)
# Установка header заголовка запроса
HEADERS = {'User-Agent': USER_AGENT.get_random_user_agent()}
URL_proxy = 'https://free-proxy-list.net/'
URL_api = 'https://api.hh.ru/vacancies'


# Метод для получения конкретной страницы пагинации
def get_page(name=None, area=113, page=0):
    """
    Создаем метод для получения страницы со списком вакансий.
    Аргументы:
        page - Индекс страницы, начинается с 0. Значение по умолчанию 0, т.е. первая страница
        area - код региона. По умолчанию 113 - Россия
        name - ключевое слово для поиска
        :return возвращает декодированный response
    """
    if name is not None:
        # Справочник для параметров GET-запроса
        params = {
            'text': f'NAME:{name}',  # Текст фильтра. В имени должно быть слово "Аналитик"
            'area': area,  # Поиск ощуществляется по вакансиям Самарская область
            'page': page,  # Индекс страницы поиска на HH
            'per_page': 100  # Кол-во вакансий на 1 странице
        }
        req = requests.get(URL_api, params, headers=HEADERS)  # Посылаем запрос к API
        data = req.content.decode()  # Декодируем его ответ, чтобы Кириллица отображалась корректно
        req.close()
        return data


# Создаем метод для получения всех страниц пагинации
def get_pages(path, name, area=113):
    """
    Функция в цикле пролучает файлы пагинации со ссылками
    :param path: Адрес расположения файлов пагинации
    :param name:Имя - ключевое слово отбора вакансий
    :param area: Укажите числовой код региона - 1586 - Самарская область, 113 - Россия. Все коды в файле areas.txt
    """
    for page in range(0, 20):
        # Преобразуем текст ответа запроса в справочник Python
        js_obj = json.loads(get_page(name, area=area, page=page))

    # Сохраняем файлы в папку {путь до текущего документа со скриптом}\docs\pagination
    # Определяем количество файлов в папке для сохранения документа с ответом запроса
    # Полученное значение используем для формирования имени документа
        next_file_name = f'{path}{len(os.listdir(path))}_{name}.json'

        # Создаем новый документ, записываем в него ответ запроса, после закрываем
        f = open(next_file_name, mode='w', encoding='utf8')
        f.write(json.dumps(js_obj, ensure_ascii=False))
        f.close()

        # Проверка на последнюю страницу, если вакансий меньше 2000
        if (js_obj['pages'] - page) <= 1:
            break

        # Необязательная задержка, но чтобы не нагружать сервисы hh, оставим.
        time.sleep(random.randint(2, 5))
    print(f'Страницы поиска собраны для вакансии: {name}')
    append_log(f'Страницы поиска собраны для вакансии: {name}')


def get_by_url(url, headers):
    """
    Возвращает .json после обращения по url
    :param headers:
    :param url: url fore requests
    :return: .json
    """
    req = requests.get(f'{url}', headers=headers)
    data_vac = req.content.decode()  # Декодируем его ответ, чтобы Кириллица отображалась корректно
    data = json.loads(data_vac)
    req.close()
    return data


def get_proxy_dict(url):
    """
    Функция возвращает актуальный список бесплатных прокси серверов в виде словаря для использования в request.
    Синтаксис request предполагает перевеорачивание ключа и значения (и дописывания?)! requests.get(url, proxies={
              "http"  : http://37.144.199.227:8000,
              "https" : https_proxy,
              "ftp"   : ftp_proxy
            })
    :param url: 'https://free-proxy-list.net/'
    :return: '37.144.199.227:8000': 'http', '118.201.86.149:3128': 'https',...
    """
    proxy_dict = {}
    req = requests.get(url)
    data = req.content.decode()  # Декодируем его ответ, чтобы Кириллица отображалась корректно
    # Получаем список портов
    ports = re.findall(r'\d+[.]\d+[.]\d+[.]\d+[:]\d+', ''.join(data))
    # Получаем тип соединения
    status = re.findall(r"td class='hx'>(\w{2,3})", ''.join(data))
    req.close()
    for i, el in enumerate(status):
        proxy_dict[ports[i]] = ('https', 'http')[el == 'no']
    return proxy_dict
