import sys, os
from Processor import *

def main(input_log, hosts, hours, resources, blocked):
    process = ProcessLog(input_log, hosts, hours, resources, blocked)
    process.run()

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
