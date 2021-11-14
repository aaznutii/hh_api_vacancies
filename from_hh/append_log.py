

from datetime import datetime


# Функция для записи событий в файл log.txt. Вынесена в отдельный файл, поскольку используется всеми модулями.
def append_log(log_text):
    """
    Функция записывает сообщения об основных этапах работы программы в файл. Запись фиксирует время события.
    :param log_text: Сообщение типа string
    """
    log = f'{log_text}:{datetime.now()}\n'
    with open(r'../from_hh/data/log.txt', mode='a', encoding='utf-8') as f:
        f.write(log)



