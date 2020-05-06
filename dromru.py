# -*- coding: utf8 -*-
"""
Многопоточный парсер автомобилей
Версия: 0.1
"""
from multiprocessing.dummy import Pool as ThreadPool
from requests_html import HTMLSession
import re
from bs4 import BeautifulSoup as bs
import json

url = 'https://auto.drom.ru/bmw/3-series/'  # не включать page
thr = 5  # количество мультипотоков
max_page = 100  # пагинация, максимальная 100 по 20
file_out = 'out_drom.csv'


def requests_url(url):
    """Get запрос HTMLSession"""
    session = HTMLSession()
    r = session.get(url)
    data = r.html.find('html', first=True).html
    return data


def gen_url(url):
    """Генерируем ссылки на пагинацию"""
    if re.search('\?', url):
        return [re.sub(f'\?', f'page{row}/?', url) for row in range(1, max_page + 1)]
    else:
        return [f'{url}page{row}' for row in range(1, max_page + 1)]


def auto_url(url):
    """Извлечение ссылок на описание автомобиля. Без мультипотока thr"""
    print(f'Этап 1: начинаю проход по страницам и извлечение ссылок на авто (не более 2000 авто)')
    print(f'*' * 84)
    url_all = []
    for num, url in enumerate(gen_url(url)):
        data = requests_url(url)
        soup = bs(data, 'html.parser')
        root = soup.findAll('a', {'class': 'b-advItem'})
        if root:
            print(f'---- Страница {num} ----')
            for i in root:
                url_new = i.get('href')
                url_all.append(url_new)
                print(f'Всего:{len(url_all)} / {url_new}')
        else:
            print('break')
            break
    return url_all


def get_full(url):
    """Получаем подробную информацию об авто"""
    data = requests_url(url)
    soup = bs(data, 'html.parser')

    for tag in soup.find_all("meta"):
        if tag.get("property", None) == "og:title":
            title = (tag.get("content", None))
            title = title.split(',')[0]
            title = re.sub('Продажа', '', title).lstrip()

        if tag.get("name", None) == "description":
            description = (tag.get("content", None))

        if tag.get("name", None) == "candy.config":
            jsn = (tag.get("content", None))
            jsn = (json.loads(jsn)['cf'])
            modid = jsn['m']  # модель
            mofid = jsn['f']  # номер модели
            price = jsn['p']  # цена
            years = jsn['y']  # год
            regid = jsn['r']  # регион
            obdvs = jsn['v']  # объем двигателя
            nodoc = jsn['is_nodocs']

            rulps = jsn['fe']['w']  # 2-левый, 1-правый
            if rulps == 1:
                rulps = 'правый руль'
            else:
                rulps = 'левый руль'

            probg = jsn['fe']['bezpr']  # 1-без пробега, 0-пробег
            if probg == 0:
                probg = 'пробег по РФ'
            else:
                probg = 'без пробега по РФ'

            newbu = jsn['fe']['new']  # 2-новая, 1-б/у
            if newbu == 2:
                newbu = 'новая'
            else:
                newbu = 'б/у'

    auto_data = [title, modid, mofid, price, years, regid, obdvs, nodoc, rulps, probg, newbu, description]
    return auto_data


off_auto = 0  # 1-выключить, 0-включить
if off_auto == 0:
    all_url = auto_url(url)
    pool = ThreadPool(thr)
    print(f'Начинаю парсить данные {len(all_url)} автомобилей в мультипотоковом режиме. Ожидайте...')
    all_data = pool.map(get_full, all_url)
    print(f'Данные на {len(all_data)} авто получены. Начинаю сохранение в файл...')

    with open(file_out, 'w', encoding='utf-8') as f:
        for row in all_data:
            dat = '|'.join([str(x) for x in row])
            f.write(dat + '\n')
    print(f'Ok. Файл записан, работа завершена')
