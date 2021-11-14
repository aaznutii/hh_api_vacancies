# Импортируем виджеты. Они позволяют более гибко управлять выводом jupyter и
# создавать интерактивные элементы (Немного переделанный код автора)
import ipywidgets as widget

# Импортируем модуль вывода jupyter

from IPython import display

# Библиотека для визуализации данных
import matplotlib.pyplot as plt

# Модуль математических функций
import math

import pandas as pd
import mysql.connector


# sklearn - библиотека, содержащая набор инструментов для машинного обучения.
# feature_extraction.text извлекает признаки из текста, которые затем можно будет
# использовать в моделировании. В нашем случае моделирование не требуется. Нам нужно
# получить биграммы и их оценку
from sklearn.feature_extraction.text import TfidfTransformer, CountVectorizer


login = 'user'
db_name = 'vac'
password = input('Введите пароль:\n')

conn = mysql.connector.connect(user=login, passwd=password, database=db_name)


# Загружаем наименования вакансий
sql = 'select name from vac.vacancies'
vacancies_name = pd.read_sql(sql, conn).name

# Загружаем навыки по вакансиям
sql = """
    select
        v.name,
        skill
    from
        vac.skills s
        join vac.vacancies v
            on v.id = s.id
"""

skills = pd.read_sql(sql, conn)

# Закрываем соединение с БД
conn.close()


# Получим матрицу встрчаемости биграмм (токенов) для каждой вакансии
# В качестве стоп-слов установим грейды сотрудников, т.е. нам
# не важно Старший аналитик данных или Младший, нам важно, что он аналитик данных
# Также исключаем биграммы, которые встречаются реже, чем в 1-ой тысячной всех вакансий
cv = CountVectorizer(
    ngram_range=(2, 2),
    stop_words=['ведущий', 'главный', 'младший', 'эксперт', 'стажер', 'старший',
                'middle', 'junior', 'senior'],
    min_df=0.001
)
word_vector = cv.fit_transform(vacancies_name)

# Рассчитаем меру idf (обратная частота документа)
# Чем выше мера для конкретного токена, тем реже он встречается
idf = TfidfTransformer().fit(word_vector)

# Строим датафрейм с оценкой idf для каждого токена. Далее он понадобится для построения облака
df_idf = pd.DataFrame(idf.idf_, index=cv.get_feature_names(), columns=["idf"])

# Строим датафрейм из матрицы частоты токенов, где в качестве строк вакансии,
# токены в качестве столбцов. Далее используем его для получения списка вакансий, для
# которых встречается конкретный токен
pivot = pd.DataFrame(
    word_vector.toarray(),
    columns=cv.get_feature_names(),
    index=vacancies_name.values
)


################################################

def click_back(b):
    """
       Метод, который вызывается при клике на кнопку "Назад".
       Метод скрывает информацию о перечне вакансий из левого сайдбара и
       диаграмму популярности навыков из правого сайдбара, а также отображает
       в центральной части шаблона облако токенов.
    """
    app_layout.left_sidebar = None
    app_layout.right_sidebar = None
    app_layout.center = out_words


################################################

def print_words(x):
    """
        Метод, который вызывается при изменении значения слайдера.
        Метод отрисовывает в формате html облако токенов

        Аргумент "x" принимает справочник, который содержит состояние слайдера. Используем
        x.new, чтобы получить новое состояние слайдера.
    """

    # Получаем датафрейм, отсортированный в обратном порядке по мере idf,
    # ограниченный диапазоном значений, заданным слайдером
    ds = df_idf.sort_values(by='idf', ascending=False)[x.new[0]: x.new[1]]

    # Получаем максимальное и минимальное значения idf. Будем использовать их для установки
    # размера шрифта токена в облаке
    mx = df_idf['idf'].max()
    mn = df_idf['idf'].min()

    # Задаем общие css-стили для облака токенов
    tags = """<style>
    .tagword{
        border:1px solid #bee5eb;
        padding:1px 5px;
        display:block;
        border-radius:4px;
        color:#0c5460;
        background:#d1ecf1;
        line-height:normal;
        cursor:pointer}
    .tagword:hover{
        background:#c1dce1}
    .tag-wrapper{
        float:left;
        margin:0 5px 5px 0}
    </style>"""

    # Пробегаемся по токенам, выбранным с помощью слайдера
    for r in ds.sort_index().itertuples():

        # Масштабируем значения idf от 0 до 1 и переворачиваем их,
        # чтобы максимальное значение стало минимальным.
        if mx > mn:

            # Задаем размер шрифта и высоту токена в облаке
            fs = int((((r.idf - mn) / (mx - mn)) * -1 + 1) * 30 + 10)
            hd = math.ceil(fs / 10) * 10 + 8
        else:
            fs = 40

        # Добавляем токен в облако в формате html. Токену назначаем функцию tag_click для
        # события клика. Сама функция описана ниже за пределами данного метода
        tag_tmpl = """<div class="tag-wrapper" style="height:{height}px">
            <span onclick="tag_click(this)" class="tagword" style="font-size:{size}px">{name}</span>
        </div>"""
        tags += tag_tmpl.format(name=r.Index, size=fs, height=hd)

    # Отрисовываем облако токенов
    click_back(None)
    out_words.clear_output(wait=True)
    with out_words:
        display.display_html(tags, raw=True)


################################################

def get_detail(w):
    """
        Метод отрисовывает детелизацию по токену, а именно перечень вакансий,
        содержащих переданный токен, и топ-20 самых востребованных навыков по этим вакансиям.

        Аргумент "w" принимает значение токена, по которому кликнули
    """

    # Выводим пандосовский датафрейм со списком вакансий, содержащих токен
    out_details_vac.clear_output(wait=True)
    vacs = pivot[pivot[w] > 0].sort_index().index.unique()
    with out_details_vac:
        display.display_html(
            pd.DataFrame(data=vacs, columns=['Список вакансий:']).to_html(index=False), raw=True
        )

    # Выводим горизонтальный столбчатый график самых популярных навыков по выбранным вакансиям
    out_details_skl.clear_output(wait=True)
    skill_pvt = skills \
        .query('name in @vacs') \
        .groupby(by='skill', as_index=False) \
        .agg({'name': 'count'}) \
        .sort_values(by='name', ascending=False).head(20)
    fig_h = skill_pvt.shape[0] / 2.5  # Корректируем размер графика на
    # основании количества получившихся столбцов
    with out_details_skl:
        skill_pvt.sort_values(by='name').plot.barh(
            x='skill',
            y='name',
            figsize=(5, fig_h),
            legend=None,
            title='ТОП20 Навыков'
        )
        plt.show()

    # Скрываем облако токенов и выводим детализацию для токена
    app_layout.center = None
    app_layout.left_sidebar = out_details_vac
    app_layout.right_sidebar = out_details_skl


################################################

# Создаем несколько виджетов вывода
out_header = widget.Output()  # Сюда будем выводить виджет слайдера и кнопку возврата
out_words = widget.Output()  # Сюда будем выводить облако
out_details_vac = widget.Output()  # Сюда будем выводить перечень вакансий, содержащих токен
out_details_skl = widget.Output()  # Сюда будем выводить график навыков по вакансиям, содержащим токен

# Создаем виджет макета. Весь отчет по сути располагается в данном макете.
# Пока только с выводом в центр виджета с облаком
app_layout = widget.AppLayout(
    header=None,
    left_sidebar=None,
    center=out_words,
    right_sidebar=None,
    footer=None,
    pane_heights=[1, 5, '60px']
)

# Создаем виджет слайдера, для указания диапазона рассматриваемых токенов
sld = widget.IntRangeSlider(
    value=[0, 0],
    min=0,
    max=df_idf['idf'].count(),
    step=1,
    description='Частота:',
    layout={'width': '100%'}
)
# Назначаем слайдеру метод, который будет вызываться при изменении значения слайдера
sld.observe(print_words, names='value')

# Создаем виджет кнопки
bck_btn = widget.Button(
    description='Назад',
    button_style='info'
)
# Назначаем кнопке метод, который будет вызываться по событию клика по ней
bck_btn.on_click(click_back)

# В формате html подготовим строку, которая создаст функцию на языке javascript
# Данную функцию будем вызывать по событию клика по токену в облаке
html = """
<script>
    function tag_click(e){
        var kernel = Jupyter.notebook.kernel
        func = 'get_detail("' + e.innerText + '")'
        kernel.execute(func)
    }
</script>"""

# Выводим в виджет вывода виджеты слайдера и кнопки
with out_header:
    display.display(sld)
    display.display(bck_btn)

display.display_html(html, raw=True)  # Отображаем в ячейке ранее подготовленный html c js-функцией
display.display(out_header)  # Отображаем виджет, содержащий слайдер и кнопку
display.display(app_layout)  # Отображаем виджет макета

# Задаем новые значения диапазона слайдера, чтобы отобразить ТОП-30 самых популярных токенов
# Данная строка спровоцирует изменения, что в свою очередь вызовет установленный метод print_words,
# который формирует облако
sld.value = [sld.max - 30, sld.max]
