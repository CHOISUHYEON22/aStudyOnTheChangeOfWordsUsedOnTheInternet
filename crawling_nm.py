from bs4 import BeautifulSoup
import multiprocessing.pool
import requests
import pickle
import time
import json


class NoDaemonProcess(multiprocessing.Process):
    def _get_daemon(self): return False
    def _set_daemon(self, value): pass
    daemon = property(_get_daemon, _set_daemon)


class InclusivePool(multiprocessing.pool.Pool): Process = NoDaemonProcess


def get_soup(code: int, page: int = 1):
    request = requests.get(f'http://movie.naver.com/movie/bi/mi/pointWriteFormList.nhn?code={code}&type=after&'
                           f'isActualPointWriteExecute=false&isMileageSubscriptionAlready=false&'
                           f'isMileageSubscriptionReject=false&page={page}')
    return BeautifulSoup(request.text, 'html.parser')


def build_detailed(args):
    try: soup = get_soup(args[1], args[0])  # 0: page, 1: code
    except requests.exceptions.MissingSchema: return {}
    comment = [(v.text if v.text[1] != '관' else v.text[5:]).strip() for v in soup.select('div.score_reple > p')]
    when = [v.text for v in soup.select('dl > dt > em:nth-child(2)')]
    return dict(zip(when, comment))


def build_database(code: int):
    temp_dict, temp_soup = dict(), get_soup(code)
    if temp_soup.find('p').text == '국내 개봉 전이라 평점을 등록할 수 없습니다.': return {}
    try: range_page = dict.fromkeys(range(1, int(temp_soup.select('span')[-1].text) + 1), code).items()
    except ValueError: return {}
    for v in multiprocessing.Pool(processes=2).map(build_detailed, range_page): temp_dict = {**temp_dict, **v}
    return temp_dict


if __name__ == '__main__':
    when_com = dict()
    start = time.time()
    for v in InclusivePool(5).map(build_database, range(100000, 200000)): when_com = {**when_com, **v}
    print(time.time() - start, "초")
    with open('DATA/nm.json', 'w+') as j: json.dump(when_com, j)
    with open('DATA/nm.txt', 'w+b') as f: pickle.dump(when_com, f)
