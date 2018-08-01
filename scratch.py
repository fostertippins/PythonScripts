import Py3Functions as py3
host = "10.161.2.12"
tag = []
tag.append('uclx_pvdints[1]')
tag.append('uclx_pvdints[2]')
value = []
print (tag)
value=py3.PLC_Connection(host, tag)
# value.append(py3.PLC_Connection(host, tag[1]))
test = str(value[0])
test = test.replace('[','')
test = test.replace(']','')
print(int(value[0]))