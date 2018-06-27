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
    #return str(str(timediff[1] + timediff[2]*0.001) + " seconds")
    return timediff[1] + timediff[2]*0.001

def print_timediff(time_1, time_2):
    dt_time_1 = get_datetime_from_teiid_timestamp(time_1)
    dt_time_2 = get_datetime_from_teiid_timestamp(time_2)
    return format_timediff(get_timediff(dt_time_1, dt_time_2))

def remove_date(t_stamp):
    if t_stamp and len(t_stamp) >= 11:
        return t_stamp[11:]



q_trusted_scales = 'SELECT g_0."EASY_RETURNS_ID" AS c_0, g_0."EASY_RETURNS_SRC" AS c_1, g_0."MPE_ID" AS c_2, g_0."MPE_TYPE" AS c_3, g_0."EVENT_CODE" AS c_4, g_0."SERIAL_NBR" AS c_5, g_0."SORT_AREA" AS c_6, g_0."CAP_UPC1" AS c_7, g_0."CAP_UPC2" AS c_8, g_0."SCALE_ERROR_CODE" AS c_9, g_0."DEST_ZIP_CODE" AS c_10, g_0."CAP_UPC3" AS c_11, g_0."CAP_UPC4" AS c_12, g_0."RUN_DATETIME" AS c_13, g_0."SCALE_SOFTWARE_VERSION" AS c_14, g_0."SERVICE_TYPE_CODE" AS c_15, g_0."IBI" AS c_16, g_0."IMB" AS c_17, g_0."CAP_DEST_ZIP_CODE" AS c_18, g_0."TRACKING_DATA_SITE" AS c_19, g_0."LENGTH" AS c_20, g_0."KAFKA_TS" AS c_21, g_0."HBASE_TS" AS c_22, g_0."RAW_TS" AS c_23, g_0."IV_TS" AS c_24, g_0."MAILER_ID" AS c_25, g_0."OPER_CODE" AS c_26, g_0."IMPB" AS c_27, g_0."HEIGHT" AS c_28, g_0."SPARK_TS" AS c_29, g_0."PIC" AS c_30, g_0."WIDTH" AS c_31, g_0."CAP_WEIGHT" AS c_32, g_0."CHANNEL_APP_ID" AS c_33, g_0."INDUCT_STATION_NUMBER" AS c_34, g_0."SCAN_DATETIME" AS c_35 FROM "PPC"."PACKAGE_SCANS_T" AS g_0 WHERE g_0."MPE_TYPE" IN (?, ?, ?, ?, ?) AND g_0."HBASE_TS" > ? AND g_0."HBASE_TS" < ? AND g_0."SCAN_DATETIME" > ? AND (g_0."RUN_DATETIME" > ? OR g_0."RUN_DATETIME" IS NULL) ORDER BY c_22'

q_scan_source = 'SELECT g_0."EASY_RETURNS_ID" AS c_0, g_0."EASY_RETURNS_SRC" AS c_1, g_0."MPE_ID" AS c_2, g_0."MPE_TYPE" AS c_3, g_0."EVENT_CODE" AS c_4, g_0."SERIAL_NBR" AS c_5, g_0."SORT_AREA" AS c_6, g_0."CAP_UPC1" AS c_7, g_0."CAP_UPC2" AS c_8, g_0."SCALE_ERROR_CODE" AS c_9, g_0."DEST_ZIP_CODE" AS c_10, g_0."CAP_UPC3" AS c_11, g_0."CAP_UPC4" AS c_12, g_0."RUN_DATETIME" AS c_13, g_0."SCALE_SOFTWARE_VERSION" AS c_14, g_0."SERVICE_TYPE_CODE" AS c_15, g_0."IBI" AS c_16, g_0."IMB" AS c_17, g_0."CAP_DEST_ZIP_CODE" AS c_18, g_0."TRACKING_DATA_SITE" AS c_19, g_0."LENGTH" AS c_20, g_0."KAFKA_TS" AS c_21, g_0."HBASE_TS" AS c_22, g_0."RAW_TS" AS c_23, g_0."IV_TS" AS c_24, g_0."MAILER_ID" AS c_25, g_0."OPER_CODE" AS c_26, g_0."IMPB" AS c_27, g_0."HEIGHT" AS c_28, g_0."SPARK_TS" AS c_29, g_0."PIC" AS c_30, g_0."WIDTH" AS c_31, g_0."CAP_WEIGHT" AS c_32, g_0."CHANNEL_APP_ID" AS c_33, g_0."INDUCT_STATION_NUMBER" AS c_34, g_0."SCAN_DATETIME" AS c_35 FROM "PPC"."PACKAGE_SCANS_T" AS g_0 WHERE g_0."HBASE_TS" > ? AND g_0."HBASE_TS" < ? ORDER BY c_22'

q_hbase = 'SELECT g_0."MAILER_ID" AS c_0, g_0."SERVICE_TYPE_CODE" AS c_1, g_0."SCAN_DATETIME" AS c_2, g_0."EASY_RETURNS_ID" AS c_3, g_0."EASY_RETURNS_SRC" AS c_4, g_0."MPE_TYPE" AS c_5, g_0."RAW_TS" AS c_6, g_0."EVENT_CODE" AS c_7, g_0."IV_TS" AS c_8, g_0."SERIAL_NBR" AS c_9, g_0."SORT_AREA" AS c_10, g_0."CAP_UPC1" AS c_11, g_0."HBASE_TS" AS c_12, g_0."CAP_UPC2" AS c_13, g_0."SCALE_ERROR_CODE" AS c_14, g_0."DEST_ZIP_CODE" AS c_15, g_0."CAP_UPC3" AS c_16, g_0."CAP_UPC4" AS c_17, g_0."SCALE_SOFTWARE_VERSION" AS c_18, g_0."SPARK_TS" AS c_19, g_0."IBI" AS c_20, g_0."IMB" AS c_21, g_0."MPE_ID" AS c_22, g_0."RUN_DATETIME" AS c_23, g_0."CAP_DEST_ZIP_CODE" AS c_24, g_0."TRACKING_DATA_SITE" AS c_25, g_0."LENGTH" AS c_26, g_0."KAFKA_TS" AS c_27, g_0."OPER_CODE" AS c_28, g_0."IMPB" AS c_29, g_0."HEIGHT" AS c_30, g_0."PIC" AS c_31, g_0."WIDTH" AS c_32, g_0."CAP_WEIGHT" AS c_33, g_0."CHANNEL_APP_ID" AS c_34, g_0."INDUCT_STATION_NUMBER" AS c_35 FROM "PPC"."PACKAGE_SCANS_T" AS g_0 WHERE g_0."HBASE_TS" > ? AND g_0."HBASE_TS" < ? ORDER BY c_0, c_1'

q_oracle = 'SELECT g_0."MID" AS c_0, g_0."STC" AS c_1, g_0."START_DATE" AS c_2, g_0."END_DATE" AS c_3, g_0."CRID" AS c_4, g_0."MAIL_CLASS" AS c_5, g_0."EPS_ACCT_NUMBER" AS c_6, g_0."DEFAULT_POSTAGE" AS c_7, g_4."MAIL_CLASS_DESC" AS c_8, g_3."PROD_GRP_NAME" AS c_9 FROM "PPCACCTMGMTDB"."MA_MID_PROFILE" g_0, "PPCACCTMGMTDB"."MA_PRODUCT_GROUP_STC" g_1, "PPCACCTMGMTDB"."MA_STC_REF" g_2, "PPCACCTMGMTDB"."MA_PRODUCT_GROUP" g_3, "PPCACCTMGMTDB"."MA_MAIL_CLASS_DEF" g_4 WHERE g_1."PROD_GROUP_STC_SEQ" = g_0."PROD_GROUP_STC_SEQ" AND g_1."STC_ID" = g_2."STC_ID" AND g_1."PROD_GRP_ID" = g_3."PROD_GRP_ID" AND g_0."MAIL_CLASS" = g_4."MAIL_CLASS" ORDER BY c_0, c_1'

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

    if match[4] == q_hbase:
        exec_dict[match[2]]['query'] = 'scan source-hbase'
    elif match[4] == q_oracle:
        exec_dict[match[2]]['query'] = 'scan source-oracle'
    elif match[4] == q_scan_source:
        exec_dict[match[2]]['query'] = 'scan source'
    elif match[4] == q_trusted_scales:
        exec_dict[match[2]]['query'] = 'trusted scales'
    else:
        exec_dict[match[2]]['query'] = 'other'


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

#Provide Statistics on queries
#q1 = scan source-hbase q2=scan source-oracle q3=scan source q4=trusted scales other=all other queries
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

'''
print "*******************************"
print "Summary"
print "*******************************"
print "# Queries:\t", len(exec_dict)
print "-----------------"

print "\tTrusted Scales"
#for readability
ts = query_dict['q4']
ts_num_queries = len(ts)
print "\t\tTimes Run:\t", ts_num_queries, " times"

if len(ts) > 0:
    ts_avg = sum(ts) / len(ts)
    ts_median = ts[len(ts) / 2]
    if len(ts) % 2 == 0 and len(ts) > 0:
        ts_median = (ts[len(ts) / 2] + ts[len(ts) / 2 - 1]) / 2
    ts_min = min(ts)
    ts_max = max(ts)

    print "\t\tAverage:  \t", ts_avg, " seconds"
    print "\t\tMedian:   \t", ts_median, " seconds"
    print "\t\tMin:      \t", ts_min, " seconds"
    print "\t\tMax:      \t", ts_max, " seconds"
print "-----------------"

print "\tScan Source"
#for readability
ss = query_dict['q3']
ss_num_queries = len(ss)
print "\t\tTimes Run:\t", ss_num_queries, " times"

if len(ss) > 0:
    ss_avg = sum(ss) / len(ss)
    ss_median = ss[len(ss) / 2]
    if len(ss) % 2 == 0:
        ss_median = (ss[len(ss) / 2] + ss[len(ss) / 2 - 1]) / 2
    ss_min = min(ss)
    ss_max = max(ss)

    print "\t\tAverage:  \t", ss_avg, " seconds"
    print "\t\tMedian:   \t", ss_median, " seconds"
    print "\t\tMin:      \t", ss_min, " seconds"
    print "\t\tMax:      \t", ss_max, " seconds"
print "-----------------"

print "\tOracle"
#for readability
orcl = query_dict['q2']
orcl_num_queries = len(orcl)
print "\t\tTimes Run:\t", orcl_num_queries, " times"

if len(orcl) > 0:
    orcl_avg = sum(orcl) / len(orcl)
    orcl_median = orcl[len(orcl) / 2]
    if len(orcl) % 2 == 0:
        orcl_median = (orcl[len(orcl) / 2] + orcl[len(orcl) / 2 - 1]) / 2
    orcl_min = min(orcl)
    orcl_max = max(orcl)

    print "\t\tAverage:  \t", orcl_avg, " seconds"
    print "\t\tMedian:   \t", orcl_median, " seconds"
    print "\t\tMin:      \t", orcl_min, " seconds"
    print "\t\tMax:      \t", orcl_max, " seconds"
print "-----------------"

print "\tHBase"
#for readability
hb = query_dict['q1']
hb_num_queries = len(hb)
print "\t\tTimes Run:\t", hb_num_queries, " times"

if len(hb) > 0:
    hb_avg = sum(hb) / len(hb)
    hb_median = hb[len(hb) / 2]
    if len(hb) % 2 == 0:
        hb_median = (hb[len(hb) / 2] + hb[len(hb) / 2 - 1]) / 2
    hb_min = min(hb)
    hb_max = max(hb)

    print "\t\tAverage:  \t", hb_avg, " seconds"
    print "\t\tMedian:   \t", hb_median, " seconds"
    print "\t\tMin:      \t", hb_min, " seconds"
    print "\t\tMax:      \t", hb_max, " seconds"
print "-----------------"

print "\tOther Queries"
#for readability
oth = query_dict['other']
oth_num_queries = len(oth)
print "\t\tTimes Run:\t", oth_num_queries, " times"

if len(oth) > 0:
    oth_avg = sum(oth) / len(oth)
    oth_median = oth[len(oth) / 2]
    if len(oth) % 2 == 0:
        oth_median = (oth[len(oth) / 2] + oth[len(oth) / 2 - 1]) / 2
    oth_min = min(oth)
    oth_max = max(oth)

    print "\t\tAverage:  \t", oth_avg, " seconds"
    print "\t\tMedian:   \t", oth_median, " seconds"
    print "\t\tMin:      \t", oth_min, " seconds"
    print "\t\tMax:      \t", oth_max, " seconds"
print "-----------------"

print "*******************************"
print "\n"
'''

for q in query_dict:
    print q
    print '\n'
    print "{0:16}{1:10}{2}\n".format("start","total(s)","finalRowCount")

    for entry in query_dict[q]:
        print "{0:15} {1:9} {2}".format(entry[0],str(entry[1]),str(entry[2]))

    print "\n"

    '''
    print "executionID=   ", exec_id
    print "finalRowCount= ", exec_dict[exec_id].get('finalRowCount')
    print "start=         ", exec_dict[exec_id].get('startTime')
    print "end=           ", exec_dict[exec_id].get('endTime')
    print "total=         ", exec_dict[exec_id].get('total'), " seconds"
    print("\n")
    print "modelName=     ", exec_dict[exec_id].get('modelName')
    print "query=         ", exec_dict[exec_id].get('query')
    print("\n")
    print "[sourceQuery]"
    print exec_dict[exec_id].get('sourceCommand')
    print("\n")
    print "[pushDownQuery]"
    print exec_dict[exec_id].get('pushDownQuery')
    print("\n")
    print "==============================="
    '''
