import csv
import os
import time
import logging
import re
import sys
from collections import namedtuple
from datetime import datetime

import grequests
import pickle
import requests
from bs4 import BeautifulSoup


ParserState = namedtuple('ParserState', ['users', 'chunk_size'])

ID_STEP = 50
REQUEST_SLEEP_TIME = 60 * 10
STEPS_TO_PICKLE = 2 ** 8
USER_INFO_URL = 'http://www.kongregate.com/api/user_info.json?user_ids={ids}'
PROFILE_URL = 'http://www.kongregate.com/accounts/{}'
MEMBER_SINCE_RE = re.compile('Member Since', re.I)
SAVE_FILE = 'state.pickle'
LOGGER_NAME = 'kong_member_since'
MESSAGES = {
    'FILE_START':           'Starting with %s',
    'REMOVED_USERS':        '%d removed, from %d to %d',
    'CURRENT_STATE':        'Chunk size: %d - Remaining users: %d - Chunks removed: %d',
    'PERCENTAGE_COMPLETE':  '%.2f%% completed, %d users remaining',
    'TIME_ELAPSED':         'Time elapsed for %s: %d',
    'PICKLING':             'Saving data',
    'START_DEDUCING':       'Starting with %s',
    'USER_DATE_DEDUCED':    'Setting %s for user %d',
}


class User(object):

    def __init__(self, id, username, member_since=None):
        self.id = id
        self.username = username
        self.member_since = member_since
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

            logger.warn('Going to sleep...')
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
    with open(path, 'r', encoding='utf-8') as f:
        for row in f:
            line = row.replace('\n', '').split(',')
            users.append(User(int(line[0]), line[1]))
    return users


def nullable_strptime(date):
    return date.strftime('%Y-%m-%d') if date is not None else ''


def pickle_state(state):
    with open(SAVE_FILE, 'wb') as f:
        pickle.dump(state, f)


def load_state(path):
    """
    Gets the initial state for the parser. Checks if there's one already stored, if not, sets the default.

    :param path: Path of the file of users to load if there's no state already saved.
    :return: A triple with current chunk size, saved users and users with no member since date
    """
    logger = logging.getLogger(LOGGER_NAME)

    state = None
    if os.path.exists(SAVE_FILE):
        with open(SAVE_FILE, 'rb') as f:
            state = pickle.load(f)

    if state is None:
        chunk_size = 2 ** 12
        saved_users = sorted(load_users_csv(path), key=lambda x: x.id)
        users_without_dates = saved_users[:]
    else:
        logger.debug('Chunk size: %d', state.chunk_size)
        logger.debug('Total users: %d', len(state.users))

        chunk_size = state.chunk_size
        saved_users = state.users
        users_without_dates = [user for user in state.users
                               if user.member_since is None and not user.member_since_fetched]

    return chunk_size, saved_users, users_without_dates


def save_users(path, users):
    """
    Saves every user in a csv file formatted as [id, username, member since date]

    :param path: Path of the file
    :type path: str
    :param users: Users to save
    :type users: list
    :return: None
    """
    with open(path, 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerows([user.id, user.username, nullable_strptime(user.member_since)] for user in users)


def main(args):
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('requests').propagate = False

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)

    user_data_path = args[1]
    user_with_dates_path = args[2]
    paths = [os.path.join(user_data_path, path) for path in os.listdir(user_data_path)]
    paths = (path for path in paths
             if path.replace('user_data/', '') not in os.listdir(user_with_dates_path))

    for path in paths:
        logger.info(MESSAGES['FILE_START'], path)

        chunk_size, users, users_without_dates = load_state(path)

        steps_to_pickle = STEPS_TO_PICKLE
        start_time = time.time()
        total_users = len(users)

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
                    logger.debug(MESSAGES['REMOVED_USERS'], len(chunk), first_user.id, last_user.id)

                    chunks_removed += 1
                    for user in chunk:
                        user.member_since = first_date
                else:
                    users_without_dates += chunk

                steps_to_pickle = (steps_to_pickle - 1) % STEPS_TO_PICKLE
                if steps_to_pickle == 0:
                    logger.info(MESSAGES['PICKLING'])
                    pickle_state(ParserState(users, chunk_size))

            chunk_size >>= 1
            remaining_users = len(users_without_dates)

            logger.info(MESSAGES['CURRENT_STATE'], chunk_size, remaining_users, chunks_removed)
            logger.info(MESSAGES['PERCENTAGE_COMPLETE'], (1 - remaining_users / total_users) * 100, remaining_users)

        end_time = time.time()
        logger.info(MESSAGES['TIME_ELAPSED'].format(path, end_time - start_time))

        save_users(path.replace('user_data', 'user_with_dates'), users)
        pickle_state(None)


if __name__ == '__main__':
    main(sys.argv)
