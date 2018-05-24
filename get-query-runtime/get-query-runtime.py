import re, sys
from datetime import datetime, date, time
from collections import OrderedDict

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
    #return str(timediff[0]) + " days " + str(timediff[1] + timediff[2]*0.001) + " seconds"
    return str(str(timediff[1] + timediff[2]*0.001) + " seconds")

def print_timediff(time_1, time_2):
    dt_time_1 = get_datetime_from_teiid_timestamp(time_1)
    dt_time_2 = get_datetime_from_teiid_timestamp(time_2)
    print( format_timediff(get_timediff(dt_time_1, dt_time_2)) )

#Grabs (SOURCE SRC COMMAND: endTime=(YYYY-MM-DD) (HH:MM:SS.FFF)[1] executionID=(######)[2] modelName=(NAME)[3] sourceCommand=(SQL)[4])[0]
re_src_cmd =        r"(SOURCE SRC COMMAND:\sendTime=(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d+).*executionID=(\d+).*modelName=([^\s]*).*sourceCommand=\[(.*)\])"

# Grabs (SOURCE SRC COMMAND: startTime=(YYYY-MM-DD) (HH:MM:SS.FFF)[1] executionID=(######)[2] sql=(SQL)[3])[0]
re_start_src_cmd =  r"(START DATA SRC COMMAND:\sstartTime=(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d+).*executionID=(\d+).*sql=(.*))"

#Grabs (SOURCE SRC COMMAND: endTime=(YYYY-MM-DD) (HH:MM:SS.FFF)[1] executionID=(######)[2])[0]
re_end_src_cmd =    r"(END SRC COMMAND:\sendTime=(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d+).*executionID=(\d+))"

#Dictionary to keep all the matches found based on executionID
exec_dict = OrderedDict()
log_lines = log.read()

matches_re_src_cmd = re.findall(re_src_cmd, log_lines)

for match in matches_re_src_cmd:
    if match[2] not in exec_dict:
        exec_dict[match[2]] = OrderedDict()

    exec_dict[match[2]]['modelName'] = match[3]
    exec_dict[match[2]]['sourceCommand'] = match[4]

matches_re_start_src_cmd = re.findall(re_start_src_cmd, log_lines)

for match in matches_re_start_src_cmd:
    if match[2] not in exec_dict:
        exec_dict[match[2]] = OrderedDict()

    exec_dict[match[2]]['startTime'] = match[1]
    exec_dict[match[2]]['pushDownQuery'] = match[3]

matches_re_end_src_cmd = re.findall(re_end_src_cmd, log_lines)

for match in matches_re_end_src_cmd:
    if match[2] not in exec_dict:
        exec_dict[match[2]] = OrderedDict()

    exec_dict[match[2]]['endTime'] = match[1]


for exec_id in exec_dict:
    start_ts = exec_dict[exec_id]['startTime']
    end_ts   = exec_dict[exec_id]['endTime']
    print "executionID =", exec_id
    print "start=", start_ts
    print "end  =", end_ts
    print_timediff( start_ts, end_ts )
    print("\n")
    print "modelName =", exec_dict[exec_id]['modelName']
    print("\n")
    print "[sourceQuery]"
    print exec_dict[exec_id]['sourceCommand']
    print("\n")
    print "[pushDownQuery]"
    print exec_dict[exec_id]['pushDownQuery']
    print("\n")
    print "==============================="
