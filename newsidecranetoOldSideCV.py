#region imports
import py3Functions as af
import pandas as pd
import time
#endregion
#region variables
amhs3Server='Amhs3'
amhsDB = 'bstone'
sqlGetAMHSJobsStr = """Select TOP 5 [srv_vehicle_id],[srv_error_code],[srv_last_work_type] 
                    From [bstone].[bstone].[srv_control]"""
oldSideIP = "10.161.2.12"
startTime=time.time()
craneDint = 0
#endregion
#region main
try:
    while (1):
        craneDint=0
        df = af.dfFromSQl(amhs3Server,amhsDB,sqlGetAMHSJobsStr)
        df=df.rename(columns={"srv_vehicle_id":"srv","srv_error_code":"errorCode","srv_last_work_type":"state"})
        for i in range (0,len(df.index)):
            if pd.isnull(df.errorCode.iloc[i])==True:
                craneDint = craneDint + pow(2,i) 
        af.writePLCTag("uclx_Crane_Py",craneDint,'DINT',oldSideIP)
        #print df
        #print time.time()-startTime
        time.sleep(60-((time.time()-startTime)%60))
        startTime=time.time()
except Exception:
    af.sendEmail('fostertippins@gmail.com','Code Failure','Python has quit working on LWRN','')
#endregion 