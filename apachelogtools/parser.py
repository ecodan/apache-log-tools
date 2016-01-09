__author__ = 'dan'

import os
import re

from processors.dummy_processor import DummyProcessor
from processors.dbc_link_processor import DbcLinkProcessor
from processors.minute_count_processor import MinuteCountProcessor
import importlib
import httpagentparser


def parse_line(line, parse_ua=False):
    # print("DIAG line: " + line)
    pattern = re.compile(r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) ([a-zA-Z0-9/]+)(.+)?\s*(\S*)" (\d{3}) (\S+) "(\S+)" "(.+?)" "(.+?)" (\d+)$')
    result = pattern.match(line)

    if result is None:
        return []

    ret = []
    for part in result.groups():
            ret.append(part)

    if parse_ua and len(ret[11]) > 0:
        # append UA data
        agent = httpagentparser.detect(ret[11])

        if 'os' in agent:
            if 'name' in agent['os']:
                # append OS
                ret.append(agent['os']['name'])
            else:
                ret.append('')
            if 'version' in agent['os']:
                # append OS version
                ret.append(agent['os']['version'])
            else:
                ret.append('')
        else:
            ret.append('')
            ret.append('')

        if 'browser' in agent:
            if 'name' in agent['browser']:
                # append browser
                ret.append(agent['browser']['name'])
            else:
                ret.append('')
            if 'version' in agent['browser']:
                # append browser version
                ret.append(agent['browser']['version'])
            else:
                ret.append('')
        else:
            ret.append('')
            ret.append('')

        if 'dist' in agent:
            if 'name' in agent['dist']:
                # append browser
                ret.append(agent['dist']['name'])
            else:
                ret.append('')
            if 'version' in agent['dist']:
                # append browser version
                ret.append(agent['dist']['version'])
            else:
                ret.append('')
        else:
            ret.append('')
            ret.append('')

        if 'platform' in agent:
            if 'name' in agent['platform']:
                # append platform
                ret.append(agent['platform']['name'])
            else:
                ret.append('')
            if 'version' in agent['platform']:
                # append version
                ret.append(agent['platform']['version'])
            else:
                ret.append('')
        else:
            ret.append('')
            ret.append('')

        if 'bot' in agent:
            ret.append(str(agent['bot']))
        else:
            ret.append('')

    else:
        ret.append('')
        ret.append('')
        ret.append('')
        ret.append('')
        ret.append('')
        ret.append('')
        ret.append('')
        ret.append('')
        ret.append('')

    return ret


def to_csv_line(groups):
    if groups is None:
        return ''
    else:
        ret = '\t'.join(map(str, groups))
        ret = ret.replace(',','_')
        ctr = 0

        return ret


def process(dir, file, processor, csv_out=False, parse_ua=False):
    outfile = None

    if csv_out == True:
        print('creating CSV')
        outfile = open(dir + '/' + file + ".csv", 'w')
        outfile.write('ip\tclient-id\tuser-id\tdatetime\tmethod\turl\turl_params\tblank\tresponse_code\tresponse_size\treferred\tuser_agent_raw\tdomain\tunk01\tos_name\tos_ver\tbrowser_name\tbrower_ver\tdist_name\tdist_ver\tplatform_name\tplatform_ver\tis_bot\n')

    print "  processing: " + str(dir + '/' + file)
    filef = open(dir + '/' + file)
    dirname = dir.split('/')[-1]
    linect = 0
    processed_linect = 0
    for line in filef:
        linect += 1

        if linect % 10000 == 0:
            print('... finished %i lines', linect)

        result = parse_line(line, parse_ua)

        if not result:
            print(line + " failed to parse")
            continue

        if processor.process_line(result):
            processed_linect += 1

            if csv_out == True:
                # print 'writing to CSV ' + str(result)
                outfile.write(to_csv_line(result))
                outfile.write('\n')

    if csv_out == True:
        outfile.close()

    processor.calculate()
    processor.dump(dir + '/' + file + ".calc.csv")

    print "done with process"
    return linect, processed_linect


def get_immediate_subdirectories(dir, recurse):
    files = os.listdir(dir)
    dirs = []
    for f in files:
        print "file " + f + " isDir=" + str(os.path.isdir(os.path.join(dir, f)))
        if os.path.isdir(os.path.join(dir, f)):
            dirs.append(os.path.join(dir, f))
            dirs = dirs + get_immediate_subdirectories(os.path.join(dir, f), recurse)
    return dirs


def main(in_dir='.', out_dir='.', recurse=False, file_list=None, processor_module_name='dummy_processor', processor_name='DummyProcessor', csv_out=False, parse_ua=False):
    print "in_dir=" + str(in_dir) + " | out_dir=" + str(out_dir) + " | recurse=" + str(recurse)

    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    dirs = [in_dir]
    if (recurse == True):
        for dir in dirs:
            dirs = dirs + get_immediate_subdirectories(dir, recurse)

    for dir in dirs:
        print "processing: " + str(dir)
        dirCt = 0
        processedDirCt = 0
        files = os.listdir(dir)
        for file in files:
            filepath = dir + '/' + file
            print filepath
            if ((file_list is not None) and (filepath not in file_list) ):
                continue
            if file.endswith('gz') == True:
                continue

            # dynamically instantiate class
            module = importlib.import_module(processor_module_name, 'processors')
            class_ = getattr(module, processor_name)
            processor = class_()

            lineCt, processedCt = process(dir, file, processor, csv_out, parse_ua)
            dirCt += lineCt
            processedDirCt += processedCt

            print "  finished processing " + str(file) + " | lines = " + str(lineCt) + " | processed=" + str(processedCt)

        print "finished dir " + str(dir) + " | lines = " + str(dirCt)+ " | processed=" + str(processedDirCt)



if __name__=='__main__':
    args = { 'in_dir':  '/Users/dan/Downloads/kd-deapp01_3_5.log',
             'out_dir': '/Users/dan/Downloads/kd-deapp01_3_5.log/out',
             'recurse': False,
             'file_list': ['/Users/dan/Downloads/kd-deapp01_3_5.log/kd-deapp01_3_5.log'],
             'processor_module_name' : 'processors.dbc_link_processor',
             'processor_name' : 'DbcLinkProcessor',
             'csv_out':True,
             'parse_ua':True
    }
    model = main(**args)


