# This is a work-in-progress. Need to refactor and add functionality

import re, sys
from datetime import datetime, date, time

log_file = 'teiid-command.log'
if len( sys.argv ) > 1:
    log_file = sys.argv[1]


log = open(log_file)

def get_value_from_key_value_pair(key_value_pair):
    if '=' not in key_value_pair:
        print('Error: not in format \'KEY=VALUE\'')
        return
    return key_value_pair.split('=')[1]

def get_datetime_from_teiid_timestamp(teiid_timestamp):
    #Parse date from timestamp
    #ex: ['2018-05-03', '08:29:55.568']
    teiid_date_list = teiid_timestamp.split()
    teiid_date = teiid_date_list[0].split('-')
    teiid_year = int(teiid_date[0])
    teiid_month = int(teiid_date[1])
    teiid_day = int(teiid_date[2])

    #Parse time from timestamp
    teiid_time_list = teiid_date_list[1].split(':')
    teiid_hour = int(teiid_time_list[0])
    teiid_minutes = int(teiid_time_list[1])
    #TODO: Hard coded the index for seconds and fractional seconds
    teiid_seconds = int(teiid_time_list[2][0:2])
    teiid_fractional_seconds = int(teiid_time_list[2][3:6])

    #Create date and time objects
    return_date = date(teiid_year, teiid_month, teiid_day)
    return_time = time(teiid_hour, teiid_minutes, teiid_seconds, teiid_fractional_seconds)

    return_datetime = datetime.combine(return_date, return_time)

    return return_datetime

def get_timediff(time_1, time_2):
    d = abs(time_2 - time_1)
    return [d.days, d.seconds, d.microseconds%1000]

def format_timediff(timediff):
    return str(timediff[0]) + " days " + str(timediff[1] + timediff[2]*0.001) + " seconds"

def print_timediff(time_1, time_2):
    dt_time_1 = get_datetime_from_teiid_timestamp(time_1)
    dt_time_2 = get_datetime_from_teiid_timestamp(time_2)
    print( format_timediff(get_timediff(dt_time_1, dt_time_2)) )
    print("\n")


start_end = [None,None]
print("\n")
print("\n")
for line in log:
        fields = re.split(r'\t+', line)

        #TODO: Let's find a better way to do this
        if len(fields) > 2:
            #Only interested in INFO
            if 'INFO' in fields[0]:
                    print(fields[2])
                    if start_end[0] is None:
                        start_end[0] = get_value_from_key_value_pair(fields[2])
                    elif start_end[1] is None:
                        start_end[1] = get_value_from_key_value_pair(fields[2])
                        print_timediff(start_end[0], start_end[1])
                        start_end = [None, None]
                    else:
                        start_end[0] = get_value_from_key_value_pair(fields[2])
                        start_end[1] = None
log.close()
