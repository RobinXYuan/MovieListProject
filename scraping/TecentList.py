import requests
# from bs4 import BeautifulSoup
from pyquery import PyQuery as pq
from urllib.parse import urlencode
import pymongo
from multiprocessing.pool import Pool


base_url = 'https://v.qq.com/x/list/movie?'

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'
}

# Connect to MongoDB
client = pymongo.MongoClient(host='localhost', port=27017, connect=False)
db = client['MovieComSite']
collection = db['TencentMovies']


def get_movie_info(base_url, offset=0):

    params = {
        'sort': 16,
        'offset': offset
    }

    url = base_url + urlencode(params)
    r = requests.get(url, headers=headers)

    if r.status_code == requests.codes.ok:
        html = r.text
        doc = pq(html)

        movie_list = list()

        # Get all list_item
        list_items = doc('.list_item').items()

        for item in list_items:
            # Define a dictionary to store movie information
            movie = {}

            # Get movie title
            movie['title'] = item.find('.figure_title a').text()

            # Get movie score
            movie['score'] = float(item.find('.figure_score .score_l').text() + item.find('.figure_score .score_s').text())

            # Get movie actors
            movie['actors'] = item.find('.figure_desc a').text()

            movie_list.append(movie)

        return movie_list

    else:
        return None


def get_multi_page_movie_info(offset):

    movies = get_movie_info(base_url, offset=offset)

    collection.insert_many(movies)
    print(movies)

    return movies


if __name__ == '__main__':
    pool = Pool()
    groups = ([x * 20 for x in range(167)])
    pool.map(get_multi_page_movie_info, groups)
    pool.close()
    pool.join()


