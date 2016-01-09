__author__ = 'dan'


class DummyProcessor():

    def __init__(self):
        None

    def process_line(self, groups):
        return True

    def calculate(self):
        return None

    def get_calculated_result(self):
        return None

    def dump(self, file_path):
        None