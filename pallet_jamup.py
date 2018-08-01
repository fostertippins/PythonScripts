import Py3Functions as py3
import server_consts as c
from Py3Functions import SQL_Container
import pandas as pd
import numpy as np

host = '10.161.2.12'
index_tag = ['uclx_pallet_jam_container[63]']
fosters_sql: SQL_Container = py3.SQL_Container(c.fosters_server, c.fosters_db, "")


def is_even(num):
    if num % 2 == 0:
        return True
    else:
        return False


def main():
    df = pd.DataFrame(columns=['pallet_number', 'jam_location'], dtype=np.int64)
    df_single = df
    df_single.loc[0] = [0, 0]
    while 1:
        # get plc array size
        index_read_tag = py3.PLC_Connection(host, index_tag)
        index_num = str(index_read_tag).replace('[', '')
        index_num = index_num.replace(']', '')
        index = int(index_num)
        df = df.iloc[0:0]
        for i in range(0, index):
            tag_to_read = [f'uclx_pallet_jam_container[{i}]']
            tag = py3.PLC_Connection(host, tag_to_read)
            write_tag = [f'uclx_pallet_jam_container[{i}]=(int)0']
            py3.PLC_Connection(host, write_tag)
            str_tag = str(tag)
            str_tag = str_tag.replace('[', '')
            str_tag = str_tag.replace(']', '')
            if is_even(i):
                df_single.pallet_number.iloc[0] = str_tag
            else:
                df_single.jam_location.iloc[0] = str_tag
                df.append(df_single)
        py3.PLC_Connection(host, ['uclx_pallet_jam_container[63]=(int)0'])
        py3.dfToSQL(fosters_sql, df, 'tbl_pallet_jam_data', index=False)
        py3.sleep(60)  # run every 10 minutes


if __name__ == '__main__':
    main()
