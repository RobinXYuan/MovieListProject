import requests
from pyquery import PyQuery as pq
import re
from multiprocessing.pool import Pool
import pymongo

base_url = 'https://movie.douban.com/j/new_search_subjects?sort=T&range=0,300&tags=&start='

headers = {
    'Host': 'movie.douban.com',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
}

client = pymongo.MongoClient(host='localhost', port=27017, connect=False)
db = client['MovieComSite']
collection = db['DoubanMovies']


def get_page(b_url, offset=0):

    url = b_url + str(offset)

    try:
        session = requests.Session()
        response = session.get(url)
        if response.status_code == requests.codes.ok:
            print('Connection OK')
            return response.json()
    except requests.ConnectionError:
        print('Connection Failed')
        return None


def get_movie_list(json):

    if json.get('data'):
        for item in json.get('data'):

            title = item.get('title')

            try:
                rate = float(item.get('rate'))
            except ValueError:
                rate = item.get('rate')

            url = item.get('url')
            yield {
                'title': title,
                'rate': rate,
                'url': url
            }

    else:
        return None


def get_movie_details(movie_item):

    movie = {}

    response = requests.get(movie_item.get('url'))

    movie['title'] = movie_item.get('title')
    movie['rate'] = movie_item.get('rate')

    if response.status_code == requests.codes.ok:

        html = response.text
        doc = pq(html)

        try:
            year = int(re.search('\d+', doc('h1 .year').text()).group())  # Get the show year
        except AttributeError:
            year = None

        # info = str(doc('#info span'))

        # types = re.findall('<span\sproperty.*?>(\w{2,3})</span>', info, re.S) # Get types of the movie

        info_texts = doc('#info').text().split('\n')

        for i in range(len(info_texts)):
            info_texts[i] = info_texts[i].split(': ')
            if len(info_texts[i]) != 2:
                info_texts[i] = None

        while None in info_texts:
            info_texts.remove(None)

        info_texts = dict(info_texts)

        try:
            movie['director'] = info_texts['导演'].split(' / ')
        except KeyError:
            movie['director'] = None

        movie['year'] = year

        try:
            movie['types'] = info_texts['类型'].split(' / ')
        except KeyError:
            movie['types'] = None

        try:
            movie['actors'] = info_texts['主演'].split(' / ')
        except KeyError:
            movie['actors'] = None

        try:
            movie['language'] = info_texts['语言']
        except KeyError:
            movie['language'] = None

        try:
            movie['region'] = info_texts['制片国家/地区']
        except KeyError:
            movie['region'] = None

        return movie

    else:

        return None


def get_movies_info(movie_list):

    movies = []

    for movie_item in movie_list:

        movies.append(get_movie_details(movie_item))

    return movies


def get_all_movies_info(offset):

    json_file = get_page(base_url, offset=offset)
    movie_list = get_movie_list(json_file)

    movies_info = get_movies_info(movie_list)
    print(movies_info)
    collection.insert_many(movies_info)

    return movies_info


if __name__ == '__main__':
    pool = Pool()
    groups = ([x * 20 for x in range(300)])
    pool.map(get_all_movies_info, groups)
    pool.close()
    pool.join()
