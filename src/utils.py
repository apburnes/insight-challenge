import os, re, time
from datetime import datetime

def create_epoch(date_string, str_format='%d/%b/%Y:%H:%M:%S'):
    epoch = datetime.strptime(date_string,str_format).strftime('%s')
    return float(epoch)

def diff_epoch(end, start, minutes=False):
    diff = end - start

    if minutes == True:
        return divmod(diff, 60)[0]
    else:
        return diff

def diff_time(end, start, unit='Minutes'):
    total = datetime.strptime(end,'%d/%b/%Y:%H:%M:%S') - datetime.strptime(start,'%d/%b/%Y:%H:%M:%S')
    total_seconds = total.days * 86400 + total.seconds

    if unit == 'Minutes':
        return divmod(total_seconds, 60)[0]
    else:
        return total_seconds

def epoch_to_string(epoch, str_format='%d/%b/%Y:%H:%M:%S'):
    return time.strftime(str_format, time.localtime(epoch))

def normpath(path):
    return os.path.normpath(os.path.join(os.getcwd(), path))

def parse_line(line):
    parsed = line.split('- -')
    host = parsed[0].strip()
    date = parsed[1][parsed[1].find('[')+1:parsed[1].find(']')]
    epoch = create_epoch(date.split(' ')[0].strip())
    request = re.findall('"([^"]*)"',parsed[1].strip())[0].split(' ')
    http_method = None if len(request) < 2 else request[0]
    http_type = None if len(request) < 3 else request[2]
    resource = request[0] if len(request) < 2 else request[1]
    req_meta = parsed[1].split('"')[-1].strip().split(' ')
    status_code = req_meta[0]

    try:
        req_size = int(req_meta[1])
    except:
        req_size = 0

    return {
        'date': date,
        'epoch': epoch,
        'host': host,
        'http_method': http_method,
        'http_type': http_type,
        'resource': resource,
        'req_size': req_size,
        'status_code': status_code
    }
