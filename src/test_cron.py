from unittest import TestCase
from src.source import load_data_sql


class Test(TestCase):

    def load_data_sql(self):
        actual = load_data_sql(1)
        self.assertTrue(actual, 'Failed inserting record into table')
