import os

from Functions import open_key_words_list, write_json_bu_url_from_db, get_df_from_pagination, \
    write_data_to_db_from_vac_files, write_areas_json, show_areas
from append_log import append_log
from Download import get_pages
from DB import create_db_table_from_df, get_df_from_db

PATH_pagination = r'../from_hh/docs/pagination/'
PATH_vacancies = r'../from_hh/docs/vacancies/'
MODE = ['append', 'replace']  # 'replace' or 'append' - Замена или дополнение базы данных


def main():
    """
    Функция для консольного запуска
    """
    # Словарь с параметрами автозапуска скрипта: '' - Выбор действий.
    user_mode = {'start_pag': '', 'start_vac': '', 'get_tables': '', 'get_select': 'b', 'get_areas': 'b'}

    append_log('Начало работы программы')
    # Файл с необходимыми названиями вакансий превращаем в list если не пусто. В противном случае возвращает None
    # Список преобразуется во множество, чтобы гарантировать отсутствие дублей.
    names = [el.strip('\n').split(':')[0] for el in (open_key_words_list('r')) if el.strip('\n').split(':')[1] == 'True']
    area = 113  # 113 код России!
    # Создаем список для сохранения ранее обработанных ключевых слов
    new_professions_list = [el for el in open_key_words_list('r') if el.strip('\n').split(':')[1] != 'True']

    # Получаем файлы пагинации
    if names is None:
        print('Обновите при необходимости key_words_list.txt. Новые файлы не созданы.')
        append_log('Новые файлы не созданы. Причина: None')
    else:
        for name in names:
            # Отметка об обработке ключевых слов и сохранение нового статуса ключевого слова в список
            print(f'Ожидаются файлы для ключевого слова: {name}')
            append_log(f'Ожидаются файлы для ключевого слова: {name}')
            get_pages(PATH_pagination, name, area)
            new_professions_list.append(f'{name}:{False}\n')
    # Записываем обновленный файл с отметкой True / False для возможности последующей работы.
    open_key_words_list('w', new_professions_list)

    # Создаем/дополняем таблицу vacancies в базе данных vac по результатам обработки страниц пагинации
    # если они есть в целевой папке
    if len(os.listdir(PATH_pagination)) > 0:
        while user_mode["start_pag"] == "":
            user_mode["start_pag"] = input(f"Обработка файлов пагинации для заполнения таблицы vacancies:\n"
                                           f"По умолчанию установлено значение mode={MODE[0]}.\n"
                                           f"Введите 0, чтобы дописать данные или 1, чтобы переписать таблицу.\n"
                                           f"Введение другого символа приведет к пропуску задачи: \n")
            if user_mode["start_pag"] == '0':
                create_db_table_from_df(get_df_from_pagination(PATH_pagination), mode=MODE[0])
            elif user_mode["start_pag"] == '1':
                create_db_table_from_df(get_df_from_pagination(PATH_pagination), mode=MODE[1])
            else:
                print('Задача пропущена\n')
                break

    # Собираем файлы вакансий на основе url из таблицы vacancies у которых значение поля add_file_vac = 0.
    # Для собранных вакансий устанавливается значение поля add_file_vac = 1
    while user_mode['start_vac'] == '':
        user_mode['start_vac'] = input('Приступить к загрузке страниц вакансий?\n'
                                       'Введите 1, чтобы начать загрузку или любой другой символ, чтобы пропустить:\n')
        if user_mode['start_vac'] == '1':
            write_json_bu_url_from_db(PATH_vacancies)
        else:
            break

    # На основе файлов вакансий заполняем три таблицы базы данных descriptions, skills, specializations
    if len(os.listdir(PATH_vacancies)) > 0:
        while user_mode['get_tables'] == "":
            user_mode['get_tables'] = input(f"Обработка файлов вакансий.json для заполнения таблиц базы данных\n"
                                            f"По умолчанию установлено значение mode={MODE[1]}.\n"
                                            f"Введите 1, чтобы переписать или 0, чтобы дописать имеющиеся таблицы."
                                            f"Введение другого символа приведет к пропуску задачи: \n")
            if user_mode['get_tables'] == '1':
                write_data_to_db_from_vac_files(PATH_vacancies, mode=MODE[1])
            elif user_mode['get_tables'] == '0':
                write_data_to_db_from_vac_files(PATH_vacancies, mode=MODE[0])
            else:
                print('Задача пропущена\n')
                break

    # Получение запросов из базы данных
    while user_mode['get_select'] == '':
        user_mode['get_select'] = input(f"Получение данных:\n"
                                        f"Если желаете посмотреть структуру базы данных, введите 1.\n"
                                        f"Для введения запроса введите запрос:\n"
                                        f"Введите любой одиночный символ для пропуска задачи: \n")
        if user_mode['get_select'] == '1':
            tables = list(get_df_from_db(select="SHOW TABLES FROM vac;")['Tables_in_vac'])
            for table in tables:
                print(table)
                print(get_df_from_db(select=f"DESC {table};"))
                # print(f"Получение данных:\n Если желаете посмотреть структуру базы данных, введите 1.\n"
                #       f"Для введения запроса введите запрос:\n Введите 0 для пропуска задачи: \n")
            user_mode['get_select'] = ''
        elif user_mode['get_select'] != '1' and len(user_mode['get_select']) == 1:
            print('Задача пропущена\n')
            break
        else:
            print(get_df_from_db(select=user_mode['get_select']))
            user_mode['get_select'] = ''

    # Получить обновленный файл кодов регионов?.
    while user_mode['get_areas'] == '':
        user_mode['get_areas'] = input('Работа с кодами регионов\n'
                                       'Введите 1, чтобы начать загрузку таблицы кодов. \n'
                                       'Введите 0, чтобы вывести на экран таблицу кодов. \n'
                                       'введите любой другой символ, чтобы пропустить задачу:\n')
        if user_mode['get_areas'] == '1':
            write_areas_json()
        elif user_mode['get_areas'] == '0':
            show_areas()
        else:
            break


if __name__ == '__main__':
    main()
