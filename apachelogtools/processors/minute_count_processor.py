__author__ = 'dan'

import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta

class MinuteCountProcessor():

    def __init__(self):
        events = []
        indices = [0,3,5,8,13]
        dfg = None

    def extract_method(self, url):
        if not url:
            return ''
        if url.startswith('/iphone/api/v1/'):
            return '/'.join(url.split('/')[0:5])
        if url.startswith('/iphone/api/v2/'):
            return '/'.join(url.split('/')[0:5])
        if url.startswith('/api/'):
            return '/'.join(url.split('/')[0:4])


    def process_line(self, line):
        # print('processing ' + str(line))
        if line is None or len(line) < 14:
            print('invalid line: ' + str(line))
            return
        self.events.append([line[i] for i in self.indices])


    def calculate(self):
        print('calculating for %i rows...', len(self.events))
        df = pd.DataFrame(np.array(self.events), columns=['ip','adate','url','status','size'])
        df["time"] = df['adate'].apply(lambda x: datetime.strptime(x[:-6], "%d/%b/%Y:%H:%M:%S"))
        df['urlc'] = df['url'].apply(lambda x: self.extract_method(x))
        dfg = df.groupby([pd.Grouper(freq='T',key='time'),'urlc']).count()
        print(str(dfg.unstack().fillna(0)))
        return dfg.unstack()['url'].fillna(0)

    def get_calculated_result(self):
        return self.dfg

    def dump(self, file_path):
        if self.dfg != None:
            self.dfg.to_csv(dir + '/' + file + ".df.csv")