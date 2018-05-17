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

#Grabs the start time and groups into timestamp, date, hours, minutes, total seconds, whole seconds, fractional seconds
#Groups requestID in order to match start and end times
re_start = r"(START USER COMMAND:\tstartTime=((\d{4}-\d{2}-\d{2}) ((\d+):(\d+):((\d+).(\d+)))))\srequestID=(.{12}\.\d+)"
re_end   = r"(END USER COMMAND:\tendTime=((\d{4}-\d{2}-\d{2}) ((\d+):(\d+):((\d+).(\d+)))))\srequestID=(.{12}\.\d+)"

#Dictionary to keep all the matches found based on request id
request_dict = {}
log_lines = log.read()

matches_re_start = re.findall(re_start, log_lines)

#Add requestID as key, and create a list with the match as first element to value
for match in matches_re_start:
    if match[-1] in request_dict:
        print "Duplicate Request ID Found: ", match[-1]
    if match[-1] not in request_dict:
        request_dict[match[-1]] = [match]

matches_re_end = re.findall(re_end, log_lines)

for match in matches_re_end:
    if match[-1] in request_dict:
        if len(request_dict[match[-1]]) >= 2:
            print "Request ID: ", match[-1], "already has a start/end pair: "
            print request_dict[match[-1]]
        else:
            request_dict[match[-1]].append(match)

print("\n\n")

for req in request_dict.keys():
    start_ts = request_dict[req][0][1]
    end_ts   = request_dict[req][1][1]
    print "startTime=", start_ts
    print "endTime  =", end_ts
    print_timediff( start_ts, end_ts )
