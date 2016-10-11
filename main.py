import os
import time
import logging
import re
import sys
from datetime import datetime

import grequests
import requests
from bs4 import BeautifulSoup

ID_STEP = 50
REQUEST_SLEEP_TIME = 60 * 2
USER_INFO_URL = 'http://www.kongregate.com/api/user_info.json?user_ids={ids}'
PROFILE_URL = 'http://www.kongregate.com/accounts/{}'
MEMBER_SINCE_RE = re.compile('Member Since', re.I)


class User(object):

    def __init__(self, id, username):
        self.id = id
        self.username = username
        self.member_since = None
        self.member_since_fetched = False

    def fetch_member_since(self):
        if self.member_since_fetched:
            return

        url = PROFILE_URL.format(self.username)

        try:
            r = requests.get(url)
            html = BeautifulSoup(r.text, 'html.parser')
            vitals = html.find(id='profile_user_vitals')
            if vitals is not None:
                a = vitals.find(string=MEMBER_SINCE_RE)
                member_since = a.parent.find_next_sibling('span')
                self.member_since = datetime.strptime(member_since.string, '%b. %d, %Y')

            self.member_since_fetched = True
        except requests.exceptions.ConnectionError as e:
            logger = logging.getLogger()
            logger.exception('')

            print('Going to sleep...')
            time.sleep(REQUEST_SLEEP_TIME)  # Wait some time to stop getting denied responses
            self.fetch_member_since()

    def previous_users(self, users):
        return [user for user in users if user.id < self.id]

    def next_users(self, users):
        return [user for user in users if user.id > self.id]

    def __repr__(self):
        return '<{}, {}>'.format(self.id, self.username)


def handler(urls):
    def f(request, exception):
        print(exception)
        urls.append(request.url)
    return f


def load_users_http(max_connections=100):
    last_id = 10005000
    ids = [[user_id for user_id in range(x, x + ID_STEP)] for x in range(10000000, last_id, ID_STEP)]
    urls = [USER_INFO_URL.format(ids=','.join(str(user_id) for user_id in id_chunk)) for id_chunk in ids]
    saved_users = {}

    while urls:
        rqs = (grequests.get(url, timeout=2) for url in urls)
        urls = []
        rs = [rq for rq in grequests.map(rqs, exception_handler=handler(urls), size=max_connections) if rq is not None]

        for r in rs:
            if r.status_code > 299:
                continue  # TODO

            json = r.json()
            success = json.get('success', False)

            if success:
                users = json.get('users', [])
                for user in users:
                    user_id = user.get('user_id')
                    username = user.get('username')

                    saved_users[user_id] = {'username': username}
            else:
                pass  # TODO

    return saved_users


def load_users_csv(path):
    users = []
    with open(path, 'r', encoding='utf-7') as f:
        for row in f:
            line = row.replace('\n', '').split(',')
            users.append(User(int(line[0]), line[1]))
    return users


def nullable_strptime(date):
    return date.strftime('%Y-%m-%d') if date is not None else ''


def main(args):
    logging.basicConfig(filename='member_since.log', level=logging.DEBUG)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logger = logging.getLogger()

    user_data_path = args[1]
    user_with_dates_path = args[2]
    paths = [os.path.join(user_data_path, path) for path in os.listdir(user_data_path)]
    paths = (path for path in paths
             if path.replace('user_data/', '') not in os.listdir(user_with_dates_path))

    for path in paths:
        logger.info('Starting with {}'.format(path))

        start_time = time.time()
        chunk_size = 2 ** 12
        saved_users = sorted(load_users_csv(path), key=lambda x: x.id)
        users_without_dates = saved_users[:]
        total_users = len(saved_users)

        while users_without_dates and chunk_size > 1:
            user_chunks = [users_without_dates[x:x+chunk_size] for x in range(0, len(users_without_dates), chunk_size)]
            users_without_dates[:] = []

            chunks_removed = 0
            for chunk in user_chunks:
                first_user, last_user = chunk[0], chunk[-1]

                first_user.fetch_member_since()
                last_user.fetch_member_since()

                first_date, last_date = first_user.member_since, last_user.member_since

                if first_date is not None and last_date is not None and first_date == last_date:
                    logger.info('{} removed, from {} to {}'.format(len(chunk), first_user.id, last_user.id))
                    chunks_removed += 1
                    for user in chunk:
                        user.member_since = first_date
                else:
                    users_without_dates += chunk

            chunk_size >>= 1
            logger.debug('Chunk size: {} - Remaining users: {} - Chunks removed: {}'.format(chunk_size,
                                                                                            len(users_without_dates),
                                                                                            chunks_removed))
            print('{:.2f}% completed'.format((1 - len(users_without_dates) / total_users) * 100))

        end_time = time.time()
        print('Time elapsed for {}: {}'.format(path, end_time - start_time))

        def by_id(x):
            return x.id

        for user in users_without_dates:
            if user.member_since is not None:
                continue

            prev_users = filter(lambda x: x.member_since is not None, user.previous_users(saved_users))
            prev_user = max(prev_users, key=by_id, default=None)

            next_users = filter(lambda x: x.member_since is not None, user.next_users(saved_users))
            next_user = min(next_users, key=by_id, default=None)

            if prev_user and next_user and prev_user.member_since == next_user.member_since:
                user.member_since = prev_user.member_since

        with open(path.replace('user_data', 'user_with_dates'), 'w') as f:
            for user in saved_users:
                f.write('{},{},{}\n'.format(user.id, user.username, nullable_strptime(user.member_since)))


if __name__ == '__main__':
    main(sys.argv)
