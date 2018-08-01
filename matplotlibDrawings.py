import pandas as pd
import matplotlib.pyplot as plt
import sys

def replaceString2(extension, find, replace):
    f=open('pressRow.csv','r')
    filedata=f.read()
    f.close()
    newdata=filedata.replace(find,replace)
    f=open(extension,'w')
    f.write(newdata)
    f.close()

df=pd.read_csv('csv.csv',delimiter=',',encoding="utf-8")
s2 = df[' AGV']
prob=s2.value_counts(sort=False)
#print(str(prob))
prob.plot(kind='bar')
#plt.show()
plt.savefig('agvxIssues.png', bbox_inches='tight',dpi=100)
s2 = df[' PRESS_ID']
prob=s2.value_counts(sort=True)
#print(str(prob))
prob.plot(kind='bar')
axes=plt.gca()
axes.set_xlim([0,10])
#plt.show()
fig = plt.gcf()
fig.set_size_inches(18.5, 10.5)
plt.savefig('temp.png', bbox_inches='tight',dpi=100)
with open('csv.csv','r') as readfile, open('pressRow.csv','w') as writefile:
    f=readfile.read()
    writefile.write(f)
replaceString2('pressRow.csv','-0','-')
replaceString2('pressRow.csv','-1','-')
replaceString2('pressRow.csv','-2','-')
replaceString2('pressRow.csv','-0D','')
replaceString2('pressRow.csv','-1D','')
replaceString2('pressRow.csv','-2D','')
replaceString2('pressRow.csv','-3D','')
replaceString2('pressRow.csv','-4D','')
replaceString2('pressRow.csv','-5D','')
replaceString2('pressRow.csv','-6D','')
replaceString2('pressRow.csv','-7D','')
replaceString2('pressRow.csv','-8D','')
replaceString2('pressRow.csv','-9D','')
replaceString2('pressRow.csv','-D','')
replaceString2('pressRow.csv','-3','')
df = pd.read_csv('pressRow.csv',delimiter=',',encoding="utf-8")
s2 = df[' PRESS_ID']
prob=s2.value_counts(sort=True)
prob.plot(kind='bar')
plt.savefig('pressRow.png', bbox_inches='tight',dpi=100)
#plt.show()