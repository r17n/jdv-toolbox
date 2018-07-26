import re, sys
from datetime import datetime, date, time
from collections import OrderedDict
from numpy import median, average

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
    return timediff[1] + timediff[2]*0.001

def print_timediff(time_1, time_2):
    dt_time_1 = get_datetime_from_teiid_timestamp(time_1)
    dt_time_2 = get_datetime_from_teiid_timestamp(time_2)
    return format_timediff(get_timediff(dt_time_1, dt_time_2))

def remove_date(t_stamp):
    if t_stamp and len(t_stamp) >= 11:
        return t_stamp[11:]

def print_stats(q_dict):
    q_num = 0

    for q in q_dict:
        times_run_actual = len(q_dict[q])
        times_run_nonzero = None
        avg_nonzero = None
        min_nonzero = None
        max_nonzero = None
        avg_nonzero_frc = None  #Average finalRowCount
        median_nonzero_frc = None #Median finalRowCount
        nonzero_list = []
        nonzero_list_times = [] #List of the total runtimes from 

        for entry in q_dict[q]:
            if entry[2] != '0' and entry[2] is not None:
                nonzero_list.append(entry)
        
        times_run_nonzero = len(nonzero_list)

        #Calulate nonzero avg, median, min, max
        if times_run_nonzero and times_run_nonzero > 0 and nonzero_list[1]:

            #Calculate Average
            avg_nonzero = average( [x[1] for x in nonzero_list] )

            #Calculate Median
            median_nonzero = median( [x[1] for x in nonzero_list] )
         
            #Fastest running query
            min_nonzero = min( val[1] for val in nonzero_list )
            
            #Slowest running query
            max_nonzero = max( val[1] for val in nonzero_list )

            #Average finalRowCount
            avg_nonzero_frc = int(average( [float(x[2]) for x in nonzero_list]))

            #Median finalRowCount
            median_nonzero_frc = int(median( [float(x[2]) for x in nonzero_list]))
        
        print '[{0}{1}]'.format('Query ',q_num)
        print '{0}'.format(q)
        print 'Times run: {0} times {1:50} Actual Times run (includes finalRowCount=0 entries): {2} times'.format(times_run_nonzero, '', times_run_actual)
        print 'Average: {0:10.3f} seconds'.format(avg_nonzero)
        print 'Median: {0:10.3f} seconds'.format(median_nonzero)
        print 'Min: {0:10.3f} seconds'.format(min_nonzero)
        print 'Max: {0:10.3f} seconds'.format(max_nonzero)
        print 'Average finalRowCount: {0} rows'.format(avg_nonzero_frc)
        print 'Median finalRowCount: {0} rows'.format(median_nonzero_frc)
        print '*****************************'

        q_num += 1
        
#Grabs (SOURCE SRC COMMAND: endTime=(YYYY-MM-DD) (HH:MM:SS.FFF)[1] executionID=(######)[2] modelName=(NAME)[3] sourceCommand=(SQL)[4])[0]
re_src_cmd =        r"(SOURCE SRC COMMAND:\sendTime=(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d+).*executionID=(\d+).*modelName=([^\s]*).*sourceCommand=\[(.*)\])"

# Grabs (SOURCE SRC COMMAND: startTime=(YYYY-MM-DD) (HH:MM:SS.FFF)[1] executionID=(######)[2] sql=(SQL)[3])[0]
re_start_src_cmd =  r"(START DATA SRC COMMAND:\sstartTime=(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d+).*executionID=(\d+).*sql=(.*))"

#Grabs (SOURCE SRC COMMAND: endTime=(YYYY-MM-DD) (HH:MM:SS.FFF)[1] executionID=(######)[2] finalRowCount=(#######)[3])[0]
re_end_src_cmd =    r"(END SRC COMMAND:\sendTime=(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d+).*executionID=(\d+).*finalRowCount=(-?\d+))"

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
    exec_dict[match[2]]['finalRowCount'] = match[3]

query_dict = dict()

for exec_id in exec_dict:
    start_ts = exec_dict[exec_id].get('startTime')
    end_ts   = exec_dict[exec_id].get('endTime')
    exec_dict[exec_id]['total'] = print_timediff( start_ts, end_ts ) if start_ts and end_ts else None

    # Check to see if we have seen this query before. If not, add it as a key to dict of queries and set value as an empty list.
    if exec_dict[exec_id].get('sourceCommand') and exec_dict[exec_id].get('sourceCommand') not in query_dict:
        query_dict[ exec_dict[exec_id].get('sourceCommand') ] = []

    # Add (start, total, finalRowCount) to list of tuples with respect to its matching key
    if exec_dict[exec_id].get('sourceCommand'):
        query_dict[ exec_dict[exec_id].get('sourceCommand')].append( (remove_date(start_ts), exec_dict[exec_id]['total'], exec_dict[exec_id].get('finalRowCount')) )

print_stats(query_dict)

for q in query_dict:
    print q
    print '\n'
    print "{0:16}{1:10}{2}\n".format("start","total(s)","finalRowCount")

    for entry in query_dict[q]:
        print "{0:15} {1:9} {2}".format(entry[0],str(entry[1]),str(entry[2]))

    print "\n"

