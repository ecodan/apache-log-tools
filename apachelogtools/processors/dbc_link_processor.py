__author__ = 'dan'

class DbcLinkProcessor():

    def __init__(self):
        None

    def process_line(self, groups):
        if (str(groups[5]).startswith('/dbc')):
            return True
        return False

    def calculate(self):
        return None

    def get_calculated_result(self):
        return None

    def dump(self, file_path):
        None