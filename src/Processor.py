import operator, os, re, time
from datetime import datetime
from utils import *

class ProcessLog(object):
    """The processing class"""
    def __init__(self, input_log, hosts, hours, resources, blocked):
        self.input_log = normpath(input_log)
        self.hosts_out = normpath(hosts)
        self.hours_out = normpath(hours)
        self.resources_out = normpath(resources)
        self.blocked_out = normpath(blocked)

        self.blocked = list()
        self.block_watch = dict()
        self.exception_count = int(0)
        self.exceptions = list()
        self.hosts = dict()
        self.hours = dict()
        self.high_hours = dict()
        self.line_count = int(0)
        self.req_count = dict()
        self.req_start = int(0)
        self.resources = dict()

    def run(self):
        with open(self.input_log, 'r') as log_file:
            for idx, line in enumerate(log_file):
                try:
                    log = parse_line(line)

                    if idx == 0:
                        self.req_start = log['epoch']

                    self.line_counter()
                    self.log_counter('req_count', log, 'epoch')
                    self.hour_counter(log['epoch'])
                    self.log_counter('hosts', log, 'host')
                    self.log_counter('resources', log, 'resource', 'req_size')
                    self.block_counter(log, line)
                except:
                    self.exception_counter()
                    self.exceptions.append(line)

        self.high_hour_counter()

        sorted_hosts = self.sort_value('hosts')
        sorted_resources = self.sort_value('resources')
        sorted_high_hours = self.sort_attr('high_hours')

        hosts = open(self.hosts_out, 'w')
        for h in sorted_hosts:
           hosts.write('{0},{1}\n'.format(h[0],h[1]))
        hosts.close()

        resources = open(self.resources_out, 'w')
        for r in sorted_resources:
           resources.write('{0}\n'.format(r[0]))
        resources.close()

        blocked = open(self.blocked_out, 'w')
        for b in self.blocked:
           blocked.write(b)
        blocked.close()

        hours = open(self.hours_out, 'w')
        for h in sorted_high_hours:
            s = epoch_to_string(h[0])
            formated_date = '{0} -0400'.format(s)
            hours.write('{0},{1}\n'.format(formated_date, h[1]))
        hours.close()

        return None

    def add_block(self, line):
        return self.blocked.append(line)

    def block_counter(self, log, line):
        host = log['host']
        epoch = log['epoch']
        http_method = log['http_method']
        status_code = log['status_code']
        resource = log['resource']

        if host in self.block_watch:
            if self.block_watch[host]['blocking'] == True:
                self.add_block(line)
                return self.clean_block(epoch)
            else:
                if status_code == '401' and resource == '/login' and http_method == 'POST':
                    self.block_watch[host] = {
                        'start': self.block_watch[host]['start'],
                        'blocking': True if (self.block_watch[host]['count'] + 1) == 3 else False,
                        'count': self.block_watch[host]['count'] + 1
                    }
                    return self.clean_block(epoch)
                elif status_code != '401' and resource == '/login' and http_method == 'POST':
                    return self.clean_block(epoch, host)
                else:
                    return self.clean_block(epoch)
        elif status_code == '401' and resource == '/login' and http_method == 'POST':
            self.block_watch[host] = {
                'start': epoch,
                'blocking': False,
                'count': 1
            }
            return self.clean_block(epoch)
        else:
            return self.clean_block(epoch)

    def clean_block(self, epoch, reset=False):
        bw = self.block_watch

        for k in bw:
            diff = diff_epoch(epoch, bw[k]['start'])

            if bw[k]['blocking'] == True and diff > float(300):
                del bw[k]
            elif bw[k]['blocking'] == False and diff > float(20):
                del bw[k]
            elif reset == k:
                del bw[k]
            else:
                pass

    def exception_counter(self):
        self.exception_count = self.exception_count + 1

    def hour_counter(self, epoch):
        date_format = '%Y%m%d%H'
        str_hour = epoch_to_string(epoch, date_format)
        hour_epoch = create_epoch(str_hour, date_format)
        self.increment('hours', hour_epoch)

    def high_hour_counter(self):
        hours = self.sort_attr('hours')

        for hour in hours:
            hour_key = hour[0]
            hour_value = hour[1]

            if hour_key < self.req_start:
                hour_key = self.req_start

            for i in range(3600):
                update_high_hours = dict()
                attr = hour_key+i

                for s in range(3600):
                    sec = attr + s
                    if sec in self.req_count:
                        increment = self.req_count[sec]
                        self.increment('high_hours', attr, increment)

                update_sorted = self.sort_attr('high_hours')

                for hr in update_sorted:
                    update_high_hours[hr[0]] = hr[1]

                self.high_hours = update_high_hours

    def increment(self, attr_key, log_key, increment=1):
        attr = getattr(self, attr_key)

        if log_key in attr:
            attr[log_key] = attr[log_key] + increment
        else:
            attr[log_key] = increment

    def line_counter(self):
        self.line_count += 1

    def log_counter(self, attr_key, log, log_key, add_attr=None):
        attr = getattr(self, attr_key)
        increment = 1 if add_attr == None else log[add_attr]

        if log[log_key] in attr:
            attr[log[log_key]] = attr[log[log_key]] + increment
        else:
            attr[log[log_key]] = increment

    def sort_attr(self, attr_key, count=10, reverse=True, sort_by=1):
        attr = getattr(self, attr_key)

        if count == False and reverse == True:
            return sorted(attr.items(), key=lambda x: (x[0], -x[1]))
        elif count != False and reverse == True:
            return sorted(attr.items(), key=lambda x: (x[0], -x[1]))[:count]
        elif count != False and reverse == False:
            return sorted(attr.items(), key=lambda x: (x[0], x[1]))[:count]
        else:
            return sorted(attr.items(), key=lambda x: (x[0], x[1]))

    def sort_value(self, attr_key, count=10, reverse=True):
        attr = getattr(self, attr_key)
        values_sorted = sorted(attr.items(), key=operator.itemgetter(1))

        if reverse == True:
            return list(reversed(values_sorted))[:count]
        else:
            return values_sorted[:count]
