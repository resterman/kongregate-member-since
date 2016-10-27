import unittest

from deduce_dates import *


def string_to_date(datestr):
    return datetime.strptime(datestr, '%Y-%m-%d')


class TestDeduceDates(unittest.TestCase):

    def setUp(self):
        self.user_1 = User(1, 'asdas')
        self.user_2 = User(2, 'ojiqwe', string_to_date('2014-08-21'))
        self.user_3 = User(3, 'qiojas')

    def test_has_member_since(self):
        user = User(1, 'asdasdsa')
        self.assertFalse(has_member_since(user))

    def test_search_next_user(self):
        users = [self.user_1, self.user_2, self.user_3]
        self.assertEqual(search_next_user(users, 0, has_member_since), self.user_2)
        self.assertIsNone(search_next_user(users, 1, has_member_since))
