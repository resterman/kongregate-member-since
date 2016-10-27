import csv
import logging
import os
import re
import sys
from datetime import datetime

from main import User, LOGGER_NAME, MESSAGES


def has_member_since(user):
    return user.member_since is not None


def search_next_user(users, index, condition):
    """
    Search for the next user in users starting in the index position
    that meets the condition requirements.
    :param users: Users list.
    :param index: Starting index.
    :param condition: Condition to evaluate
    :return: A user if found, else None
    """
    for i in range(index + 1, len(users)):
        if condition(users[i]):
            return users[i]


def get_users(from_path):
    users = []
    with open(from_path, 'r') as file:
        for row in csv.reader(file):
            user_id = int(row[0])
            username = row[1]
            member_since = datetime.strptime(row[2], '%Y-%m-%d') if row[2] is not '' else None
            users.append(User(user_id, username, member_since))
    return sorted(users, key=lambda u: u.id)


def save_users(folder, filename, users):
    def get_values(user):
        return [user.id, user.username, None if user.member_since is None else user.member_since.strftime('%Y-%m-%d')]

    with open(os.path.join(folder, filename), 'w') as file:
        writer = csv.writer(file)
        writer.writerows([get_values(user) for user in users])


def main(args):
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(LOGGER_NAME)

    csv.field_size_limit(sys.maxsize)

    user_with_dates_folder = args[1]
    paths = sorted(os.listdir(user_with_dates_folder), key=lambda x: int(re.search('^(\d*)_', x).group(1)))
    if not paths:
        return  # TODO
    else:
        paths = [os.path.join(user_with_dates_folder, path) for path in paths]

    save_folder = args[2]
    os.makedirs(save_folder, exist_ok=True)

    last_member_since = None
    prev_path = paths[0]
    next_users = get_users(prev_path)

    for i, _ in enumerate(paths[1:]):
        logger.info(MESSAGES['START_DEDUCING'], prev_path)

        current_users = next_users
        next_users = get_users(paths[i])

        for j, user in enumerate(current_users):
            if user.member_since is None:
                next_user = search_next_user(current_users, j, has_member_since) \
                            or search_next_user(next_users, 0, has_member_since)

                if next_user is not None and last_member_since is not None:
                    have_same_date = next_user.member_since == last_member_since
                    if have_same_date:
                        logger.debug(MESSAGES['USER_DATE_DEDUCED'], last_member_since.strftime('%Y-%m-%d'), user.id)
                        user.member_since = last_member_since
            else:
                last_member_since = user.member_since

        save_users(save_folder, os.path.basename(prev_path), current_users)
        prev_path = paths[i]


if __name__ == '__main__':
    main(sys.argv)
