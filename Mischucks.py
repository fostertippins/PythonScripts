#region imports
import Py3Functions as py3
import pandas as pd
import numpy as np
import time
#endregion
#region variable_init

etriccServer = 'S-WN-PLT-DBA05\\Etricc'
etriccDb = 'Etricc'
amhs3Server='Amhs3'
amhsDB = 'bstone'
mhcsServer='Sql-wn-mhcs'
mhcsDB = 'GreenTire'
myServer = 'APP1-WN-FOST\\SQLEXPRESS'
myDb = 'FostersDB'
sqlGetAlarmStr = """Select TOP 15 [id], [vehicle_id], [vehicle_alarm_type_id], [destination_name] From dbo.Vehicle_Alarms  where vehicle_alarm_type_id like 'Dev1:4%' order by id desc"""
sqlGetAMHSJobsStr = """Select TOP 20 [nindex],[agv],[press],[loader],[leftBarcode],[dt] From dbo.AGVHistory order by dt desc"""
sqlGetMHCSJobsStr = """Select TOP 20 [nindex],[agv],[press],[loader],[leftBarcode],[dt] From dbo.AGVHistory order by dt desc"""
sqlGetFostersStr = """Select TOP 100 [nindex],[agv],[press],[loader],[leftBarcode],[dt],[Error_Code],[productCode],[agvHistoryID],[failSide],[tireSize] From dbo.AGVTransports order by dt desc"""
sqlGetFostersAlarmStr = """Select [id], [agv], [Error_Code], [press]  From dbo.AGVHistory"""
sqlGetTireSizeData = """Select [Tire_Size], [Product_Code] From dbo.TireProductCodes"""
startTime=time.time()
lastTime=time.time()
pressIPs = ['133','134','141','142','143','144','145','146','147','148','149','150','151','152','153','154','155','156']
press=['','','','','','','','','','','','','','','','','','','','','','','','','','','','','','']           
#endregion
#region MAIN
try:
    tireSizeDB=pd.read_csv('C:\\Users\\tippinsfoste\\OneDrive\\PythonScripts\\tireData.csv',delimiter='\t')
    while(1):
    #initialize and correct all databases
        amhsSQL=py3.SQL_Container(amhs3Server,amhsDB,sqlGetAMHSJobsStr)
        mhcsSQL=py3.SQL_Container(mhcsServer,mhcsDB,sqlGetMHCSJobsStr)
        etriccSQL=py3.SQL_Container(etriccServer,etriccDb,sqlGetAlarmStr)
        fostersSQL=py3.SQL_Container(myServer,myDb,sqlGetFostersStr)

        dfA=py3.dfFromSQl(amhsSQL)
        dfA.press=dfA['press'].astype(str).str.strip()
        dfA.dt=dfA['dt'] + pd.to_timedelta(1, unit='h') # amhs3 is 1 hour begind AMHS3
        dfA.nindex=dfA['nindex'].astype(str).str.strip().astype(np.int64)
        
        dfM=py3.dfFromSQl(mhcsSQL)
        dfM.press=dfM['press'].astype(str).str.strip()
        dfM.nindex=dfM['nindex'].astype(str).str.strip().astype(np.int64)

        dfAlarms=py3.dfFromSQl(etriccSQL)
        dfAlarms=dfAlarms.rename(columns={"vehicle_alarm_type_id":"Error_Code","vehicle_id":"agv","destination_name":"press"})
        dfAlarms.press=dfAlarms['press'].astype(str).str[:-1]
        dfAlarms.press=dfAlarms['press'].astype(str).str.strip()

        dfFosAll=py3.dfFromSQl(fostersSQL)

        dfFos=dfFosAll.loc[:,['nindex','agv','press','loader','leftBarcode','dt']]
        dfFos.press=dfFos['press'].astype(str).str.strip()
        dfFos.nindex=dfFos['nindex'].astype(str).str.strip().astype(np.int64)
        dfFos=dfFos.drop_duplicates(subset='nindex',keep='last')

        fostersSQL.cmdString=sqlGetFostersAlarmStr
        dfB=py3.dfFromSQl(fostersSQL)
        dfB=pd.concat([dfB,dfAlarms])
        dfB=dfB.drop_duplicates(subset='id',keep=False)

        dfConcat = pd.concat([dfFos,dfA,dfM])
        dfConcat=dfConcat.drop_duplicates(subset ='nindex',keep=False)
        dfNew=dfConcat
    #get rid of the dfFos data we just put in...
        try:
            for i in range (0,len(dfFos.index)):
                for j in range (0,len(dfConcat.index)):
                    if str(dfConcat.nindex.iloc[j]) == str(dfFos.nindex.iloc[i]): 
                        dfNew=dfNew[dfNew.nindex!=dfConcat.nindex.iloc[j]]
                        break
                    else:
                        pass
        except IndexError:
            pass
        dfConcat=dfNew.sort_values('dt')
    #update Databases
        py3.dfToSQL(fostersSQL,dfB,'AGVHistory',append='append',index=True)
        py3.dfToSQL(fostersSQL,dfConcat,'AGVTransports',append='append',index=True)
    #reinitialize variables for this run
        fostersSQL.cmdString=sqlGetFostersStr
        dfFosAll=py3.dfFromSQl(fostersSQL)

        dfFos=dfFosAll.loc[:,['nindex','agv','press','loader','leftBarcode','dt']]
        dfFos.press=dfFos['press'].astype(str).str.strip()
        dfFos.nindex=dfFos['nindex'].astype(str).str.strip().astype(np.int64)
        dfFos=dfFos.drop_duplicates(subset='nindex',keep='last')
    #add product codes amhs
        amhsSQL.cmdString = "Select TOP 20 [Verification_Data_R_Barcode],[Verification_Data_L_Prod] From dbo.VerifyPress Order by Verification_Data_Time desc"
        dfProd = py3.dfFromSQl(amhsSQL)
        try:
            for i in range (0,20):
                for j in range (0,len(dfFos.index)):
                    if int(dfProd.Verification_Data_R_Barcode.iloc[i]) == int(dfFos.leftBarcode.iloc[j]): 
                        tireSize1=dfFos.leftBarcode.iloc[j]
                        tireSize1=tireSize1[:3]
                        fostersSQL.cmdString = "Update dbo.AGVTransports Set productCode='"+str(dfProd.Verification_Data_L_Prod.iloc[i])+"',tireSize="+tireSize1+" Where nindex="+str(dfFos.nindex.iloc[j])
                        py3.updateSQL(fostersSQL)
                        break
                    else:
                        pass
        except Exception:
            pass 
    #add product codes mhcs
        mhcsSQL.cmdString = "Select TOP 20 [Press_ID],[Left_Product_Code],[AGV_ID] From dbo.AGVAssignments Order by ID desc"
        dfProd2 = py3.dfFromSQl(mhcsSQL)
        try:
            for i in range (0,20):
                for j in range (0,len(dfFos.index)):
                    if (str(dfProd2.Press_ID.iloc[i]) == str(dfFos.press.iloc[j])) and (dfProd2.AGV_ID.iloc[i]==dfFos.agv.iloc[j]): 
                        tireSize2=dfFos.leftBarcode.iloc[j]
                        tireSize2=tireSize2[:3]
                        fostersSQL.cmdString = "Update dbo.AGVTransports Set productCode='"+str(dfProd2.Left_Product_Code.iloc[i])+"',tireSize="+tireSize2+" Where nindex="+str(dfFos.nindex.iloc[j])
                        py3.updateSQL(fostersSQL)
                        break
                    else:
                        pass
        except IndexError:
            pass
    #look for values in existing database for Existing AGV_Alarms and append alarm state
        for i in range(0,15):
            try:
                for j in range (0,len(dfFos.index)):
                    if str(dfAlarms.agv.iloc[i]) == str(dfFos.agv.iloc[j]):
                        if (str(dfAlarms.press.iloc[i]) == str(dfFos.press.iloc[j])) and (pd.isnull(dfFosAll.Error_Code.iloc[j])==True):
                            fostersSQL.cmdString = "Update  dbo.AGVTransports Set Error_Code = '"+str(dfAlarms.Error_Code.iloc[i])+"', agvHistoryID ="+str(dfAlarms.id.iloc[i])+" Where nindex = "+str(dfFos.nindex.iloc[j])
                            py3.updateSQL(fostersSQL)   
                            break
                        break
            except IndexError:
                print ('index j out of bounds')
    #write DBs to CSV
        #dfConcat.to_csv('dfAll.csv',mode='a')
    #append FailSide
        # dfError = dfFosAll.loc[:,['agvHistoryID','failSide']]
        # for i in range (0,len(pressIPs)):
        #     side,side2,press=py3.readPLCTag('Control_Bits[4]',pressIPs[i],3145728)
        #     try:
        #         for j in range (0,30):
        #             if (press == dfFosAll.press.iloc[j]) and (dfError.agvHistoryID.iloc[j]>1) and (pd.isnull(dfError.failSide.iloc[j])==True):
        #                 if side==1048576:
        #                     dropside='Right'
        #                 elif side==2097152:
        #                     dropside='Left'
        #                 elif side==0:
        #                     dropside='Both'
        #                 elif side2==4063656:
        #                     dropside='None'
        #                 else:
        #                     dropside=str(side2)
        #                 tempsqlstr = """Update dbo.AGVTransports
        #                                 Set failSide = '"""+dropside+"""'  
        #                                 Where nindex = """+str(dfFosAll.nindex.iloc[j])
        #                 py3.updateSQL(myServer,myDb,tempsqlstr)
        #                 break
        #             else:
        #                 pass
        #     except Exception:
        #         print ('index j out of bounds')
    #region AddRimSize

        try:
            for i in range(0,20):
                for j in range(0,len(tireSizeDB.index)):
                    if tireSizeDB.tireSize[j]==dfFosAll.tireSize[i]:
                        fostersSQL.cmdString = "Update dbo.AGVTransports Set rimSize = "+str(tireSizeDB.rimSize[j])+" Where nindex = "+str(dfFosAll.nindex.iloc[i])
                        py3.updateSQL(fostersSQL)
                        break
                    else:
                        pass
        except Exception:
            print ('rimSize append error')
    #endRegion
    #redo every 2 minutes
        print (time.time()-startTime)
        py3.sleep(120)
        startTime=time.time()
    #endOfDayReport
        # if time.time()-lastTime>86400:
        #     dfReadAll=py3.dfFromSQl(myServer,myDb,'Select * from dbo.AGVTransports')
        #     dfReadAll=dfReadAll.drop_duplicates(subset='nindex',keep='first')
        #     dfReadAll=dfReadAll.drop(columns=['index'])
        #     dfReadAll=dfReadAll.sort_values('dt')
        #     dfReadAll.to_csv('readall.csv',mode='w')
        #     #py3.dfToSQL(myServer,myDb,dfReadAll,'AGVTransports','append',False)
        #     #print dfReadAll.nindex.iloc[0]
        #     py3.plotAndSave(dfReadAll,'agvreport.png','agv','',0,False)
        #     py3.plotAndSave(dfReadAll,'pressreport.png','press','',10,False)
        #     py3.sendEmail('fostertippins@gmail.com;tippinsfoster@bfusa.com','DroppedTiresUpdate','Heres the update',
        #         ['C:\\Users\\TippinsFoste\\Documents\\Python Scripts\\pressreport.png','C:\\Users\\TippinsFoste\\Documents\\Python Scripts\\agvreport.png'])
        #     lastTime=time.time()
    #endregion
except Exception:
    py3.sendSMTP('fostertippins@gmail.com','FostersAlerts','AGV data gathering has stopped from Python')