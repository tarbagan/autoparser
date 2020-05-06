# -*- coding: utf8 -*-
"""
Многопоточный парсер auto.ru
Версия: 0.1
"""
from multiprocessing.dummy import Pool as ThreadPool
from requests_html import HTMLSession
from bs4 import BeautifulSoup as bs
import math

url = 'https://auto.ru/cars/toyota/corolla/all/?sort=fresh_relevance_1-desc&top_days=31&year_from=2001'  # не включать page
thr = 5  # количество мультипотоков
max_page = 99  # пагинация, максимальная 99
file_out = 'out_auto.csv'


def get_page(url):
    """Получаем количество предложений для пагинации"""
    session = HTMLSession()
    try:
        r = session.get(url)
        pag = r.html.find('.ButtonWithLoader__content', first=True).html
        res = pag.split()
        numtext = [x for x in res if x.isdigit()]
        numtext = ''.join(numtext)
        allrow = int(numtext)
        pages = allrow / 37
        pages = math.ceil(pages)
        if pages >= max_page:
            pagination = max_page
        else:
            pagination = pages
        return {'all': allrow, 'pagination': pagination, 'pages': pages}
    except Exception as e:
        print(f'Ошибка, данные для пагинации не получены: Проверьте ссылку! ({e})')
        return {'all': 0, 'pagination': 0, 'pages': 0}


def make_url(url):
    """Создаём page/pagination страницы"""
    info = get_page(url)
    all = info['all']
    pagination = info['pagination']
    if all == 0:
        return None
    else:
        print(f'Формируем пагинацию {pagination} страниц...')
        if pagination <= 1:
            m_url = url
            return m_url
        else:
            m_url = [f'{url}&page={x}' for x in range(1, pagination)]
            return m_url


def get_more(url):
    """Получаем ссылук на полное описание авто"""
    all_url = []
    session = HTMLSession()
    try:
        r = session.get(url)
        data = r.html.find('#app', first=True).html
        soup = bs(data, 'html.parser')
        block = soup.findAll('div', {'class': 'ListingItem-module__main'})
        for row in block:
            url = row.find('a').get('href')
            all_url.append(url)
        return list(set(all_url))
    except Exception as e:
        print(f'Ошибка в получении полных ссылок. Возможна блокировка {e}')


def get_full(url):
    """Извлекаем данные по авто из тегов html"""
    session = HTMLSession()
    r = session.get(url)

    data = r.html.find('#app', first=True).html
    soup = bs(data, 'html.parser')

    name = soup.findAll("meta", itemprop="name")[0].get('content')
    body = soup.findAll("meta", itemprop="bodyType")[0].get('content')
    brand = soup.findAll("meta", itemprop="brand")[0].get('content')
    color = soup.findAll("meta", itemprop="color")[0].get('content')
    fuel = soup.findAll("meta", itemprop="fuelType")[0].get('content')
    modate = soup.findAll("meta", itemprop="modelDate")[0].get('content')
    dors = soup.findAll("meta", itemprop="numberOfDoors")[0].get('content')
    prdt = soup.findAll("meta", itemprop="productionDate")[0].get('content')
    cnfc = soup.findAll("meta", itemprop="vehicleConfiguration")[0].get('content')
    tran = soup.findAll("meta", itemprop="vehicleTransmission")[0].get('content')
    price = soup.findAll("meta", itemprop="price")[0].get('content')
    cur = soup.findAll("meta", itemprop="priceCurrency")[0].get('content')

    data_auto = [name, body, brand, color, fuel, modate, dors, prdt, cnfc, tran, price, cur]
    return data_auto


# мультипоточным парсингом получаем ссылки на все авто url_auto
page_off = 0  # отладочная включалка (рабочий режим - 0)
if page_off == 0:
    pool = ThreadPool(thr)
    st_url = make_url(url)
    all_url = pool.map(get_more, st_url)
    auto_url = []
    for ar_lnk in all_url:
        for url in ar_lnk:
            auto_url.append(url)
    url_auto = list(set(auto_url))  # set первого этапа!
    print(f'Получено {len(url_auto)} ссылок. Перехожу к мультипоточному парсингу данных (кратких)...')
    print('*' * 80)

# ручной постраничный режим
off_manual = 1  # 1-выключить, 0-включить
if off_manual == 0:
    print(f'постраничный режим. Получаю данные и записываю в файл:')
    with open(file_out, 'w', encoding='utf-8') as f:
        for url in url_auto:
            data = get_full(url)
            dat = '|'.join(data)
            f.write(dat + '\n')
            print (dat)

# автоматический мультипотоковый
off_auto = 0  # 1-выключить, 0-включить
if off_auto == 0:
    print(f'Мультипотоковый режим. Получаю данные и записываю в файл. Ожидайте, данные не выводятся...')
    pool = ThreadPool(thr)
    all_data = pool.map(get_full, url_auto)
    with open(file_out, 'w', encoding='utf-8') as f:
        for row in all_data:
            dat = ('|'.join(row))
            f.write(dat + '\n')
