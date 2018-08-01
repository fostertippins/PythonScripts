import time
import Py3Functions as py3
import pandas as pd
##
host = '10.161.2.12'
msgFrom = "DownTimeAlerts"
mhcsServer = 'Sql-wn-mhcs'
mhcsDB = 'GreenTire'
sqlGetMHCSAlarmsStr = "Select TOP (2) Alarm_Text From [GreenTire].[dbo].[Alarms] where Alarm_Group_ID = 6 or " \
                      "Alarm_Group_ID = 1 or Alarm_Group_ID = 7 order by Alarm_DateTime desc"
fosterServer = 'S-WN-PLT-DBADev\Development'
fosterDB = 'FosterT'
sqlGetMembersStr = "Select * From [dbo].[ASRSMembersAlert] where PK=1 or PK=4"
messageSent = False
alarm_data_received = False
startTime = time.time()
msgTo = ""
mhcsSQL: py3.SQL_Container = py3.SQL_Container(mhcsServer, mhcsDB, sqlGetMHCSAlarmsStr)
lastAlarm = py3.dfFromSQl(mhcsSQL)
fostersSQL = py3.SQL_Container(fosterServer, fosterDB, sqlGetMembersStr)


def new_side_crane():
    amhs3_server = 'Amhs3'
    amhs_db = 'bstone'
    sql_get_amhs_jobs_str = "Select TOP 5 [srv_vehicle_id], [srv_error_code], [srv_last_work_type]" \
                            " From [bstone].[bstone].[srv_control]"
    amhs_sql = py3.SQL_Container(amhs3_server, amhs_db, sql_get_amhs_jobs_str)
    crane_dint = 0
    amhs_df = py3.dfFromSQl(amhs_sql)
    amhs_df = amhs_df.rename(columns={"srv_vehicle_id": "srv", "srv_error_code": "errorCode",
                                      "srv_last_work_type": "state"})
    for I in range(0, len(amhs_df.index)):
        if pd.isnull(amhs_df.errorCode.iloc[I]):
            crane_dint = crane_dint + pow(2, I)
    tag = ["uclx_Crane_Py=(dint)%s" % crane_dint]
    py3.PLC_Connection(host, tag)


def alerts(sent, min_time):
    global alarm_data_received, lastAlarm, msgFrom, msgTo
    tag = ['uCLX_PVdints[23]']
    clx_list = py3.PLC_Connection(host, tag)
    minutes_to_milli: int = min_time * 60 * 1000
    message_sent = sent
    if clx_list[0] > 120000:
        if not alarm_data_received:
            lastAlarm = py3.dfFromSQl(mhcsSQL)
            alarm_data_received = True
    else:
        alarm_data_received = False
    if clx_list[0] > minutes_to_milli and not sent:
        minutes_down = int((clx_list[0]/1000)/60)
        body = "OldSide ASRS down for %s minutes. Likely cause: %s or %s" % (minutes_down, lastAlarm.Alarm_Text[0],
                                                                             lastAlarm.Alarm_Text[1])
        # py3.sendSMTP(msgTo[0],msgFrom,body)
        py3.sendEmail(msgTo, '', body, '')
        message_sent = True
    elif clx_list[0] < minutes_to_milli:
        message_sent = False
    print(clx_list[0], message_sent, lastAlarm.Alarm_Text[0])
    return message_sent


# noinspection PyBroadException
try:
    members = py3.dfFromSQl(fostersSQL)
    for i in range(0, len(members.index)):
        msgTo = msgTo + members.Number[i] + ";"
    print(msgTo)
    while 1:
        new_side_crane()
        messageSent = alerts(messageSent, 5)
        py3.sleep(30)
except Exception:
    py3.sendSMTP("fostertippins@gmail.com", "Alerts@bfusa", "oldSideAlerts.py has failed")
