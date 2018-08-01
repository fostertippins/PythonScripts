#region imports
import pyodbc
import win32com.client as win32
import time
from datetime import datetime
import pandas as pd
import pandas.io.sql
import urllib
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from cpppo.server.enip import client
import smtplib
#endregion

#region functions
class SQL_Container:
    def __init__(self, server, database, cmdString):
        self.server = server
        self.database = database
        self.cmdString = cmdString
    def __repr__(self):
        return (str(self.server)+","+str(self.database)+","+str(self.cmdString))


def getTimestamp(readable):
    mytime = time.time()
    if readable == True:
        mytime = datetime.fromtimestamp(mytime)
    else:
        pass
    return mytime


def writeCSV(toFileName,append,value,beforeComma=False,row='No'):
    with open(toFileName,append) as tofile:
        if beforeComma==True:
            tofile.write(',')
        else:
            pass
        if row !='No':
            tofile.write(row)
        else:  
            tofile.write(value)
        tofile.write('\n')


def plotAndSave(df,saveFileName,x_axis,y_axis=0,top=0,sort=True):
    s2 = df[x_axis]
    prob=s2.value_counts(sort=sort)
    #print str(prob)
    prob.plot(kind='bar')
    if top!=0:
        axes=plt.gca()
        axes.set_xlim([0,top])
    plt.savefig(saveFileName, bbox_inches='tight')

def updateSQL(sql: SQL_Container):
    cnxn = pyodbc.connect('Trusted_Connection=yes', driver = '{SQL Server}',server = sql.server, database = sql.database)
    cursor = cnxn.cursor()
    cursor.execute(sql.cmdString)
    cursor.commit()
    cursor.close()

def dfFromSQl(sql: SQL_Container):
    conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + sql.server + ';DATABASE=' + sql.database + ';Trusted_Connection=yes')
    df = pandas.io.sql.read_sql(sql.cmdString, conn)
    return df

def dfToSQL(sql: SQL_Container,df,table,append='append',index=True):
    try:
        params=urllib.parse.quote_plus('DRIVER={SQL Server};SERVER='+sql.server+';DATABASE='+sql.database+';Trusted_Connection=yes')
        engine = create_engine("mssql+pyodbc:///?odbc_connect=%s"%params)
        df.to_sql(table, engine, if_exists = append,index=index)
    except Exception:
        print ('writeToSQL Error')

def dropDuplicates(sql: SQL_Container,df,table,subset,append):
    df=df.drop_duplicates(subset=subset,keep='first')
    dfToSQL(sql,df,table,append=append,index=False)

def PLC_Connection(host,tags): 
    with client.connector(host=host) as conn:
            for index,descr,op,reply,status,value in conn.pipeline(
                operations=client.parse_operations(tags),depth=2):
                    retVal=value
    return retVal

def sendEmail(recipient,subject,body,attachment):
    outlook=win32.Dispatch('Outlook.Application')
    mail=outlook.CreateItem(0)
    mail.Importance=0
    #add semicolon for multiple recipients
    mail.To=recipient
    mail.Subject=subject
    mail.Body=body
    if attachment!='':
        for i in range(0,len(attachment)):
            mail.Attachments.Add(attachment[i])
    #mail.Display()
    mail.send

def sendSMTP(msgTo,msgFrom,body):
    SERVER = "mailedge.bfusa.com"
    server=smtplib.SMTP(SERVER)
    message = """%s""" %  body
    server.sendmail(msgFrom,msgTo,message)
    server.quit()

def sleep(sleepTime):
    time.sleep(sleepTime)

def main():
    pass


if __name__ == '__main__':
    main()