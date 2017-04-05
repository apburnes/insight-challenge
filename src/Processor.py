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
        self.line_count = int(0)
        self.req_count = dict()
        self.req_start = int(0)
        self.req_total = dict()
        self.resources = dict()

    def run(self):
        with open(self.input_log, 'r') as log_file:
            for idx, line in enumerate(log_file):
                try:
                    log = parse_line(line)

                    if idx == 0:
                        self.req_start = log['epoch']

                    self.line_counter()
                    self.log_counter('hosts', log, 'host')
                    self.log_counter('resources', log, 'resource', 'req_size')
                    self.block_counter(log, line)
                    self.req_counter(log)
                except:
                    self.exception_counter()
                    self.exceptions.append(line)

        sorted_hosts = self.sort_value('hosts')
        sorted_resources = self.sort_value('resources')
        sorted_req_total = self.sort_attr('req_total')

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
        for r in sorted_req_total:
            s = epoch_to_string(r[0])
            f = '{0} -0400'.format(s)
            hours.write('{0},{1}\n'.format(f, r[1]))
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

    def line_counter(self):
        self.line_count += 1

    def log_counter(self, attr_key, log, log_key, add_attr=None):
        attr = getattr(self, attr_key)
        increment = 1 if add_attr == None else log[add_attr]

        if log[log_key] in attr:
            attr[log[log_key]] = attr[log[log_key]] + increment
        else:
            attr[log[log_key]] = increment

    def increment(self, attr_key, log_key, increment=1):
        attr = getattr(self, attr_key)

        if log_key in attr:
            attr[log_key] = attr[log_key] + increment
        else:
            attr[log_key] = increment

    def req_counter(self, log):
        epoch = log['epoch']
        diff = diff_epoch(epoch, self.req_start)

        if diff == 0.0:
            self.increment('req_count', epoch)
        elif diff > 0.0 and diff <= 3600.0:
            for i in range(int(diff)+1):
                log_key = epoch - i
                self.increment('req_count', log_key)
        else:
            for i in range(3601):
                log_key = epoch - i
                self.increment('req_count', log_key)

        self.reqs_per_hour(epoch)

    def reqs_per_hour(self, epoch):
        total = dict()
        req_counts = self.sort_attr('req_count', False)

        for req in req_counts:
            diff = diff_epoch(epoch, req[0])

            if diff <= 3600.0:
                self.req_total[req[0]] = req[1]
            else:
                del self.req_count[req[0]]

        req_total = sort_attr('req_total')

        for req in req_total:
            total[req[0]] = req[1]

        self.req_total = total

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
            return list(reversed(values_sorted))[:10]
        else:
            return values_sorted[:10]

    def traffic_counter(self):
        reqs = self.sort_attr('req_counts', False, False)
        start = reqs[0][0]
        end = reqs[-1][0]

        while start <= end:
            self.traffic[start] = 0

            for i in range(3600):
                attr = float(start+i)
                if attr in self.req_counts:
                    self.traffic[start] = self.traffic[start] + self.req_counts[start+i]

            start += 1
