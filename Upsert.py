import sys

import hana_ml
import pandas as pd
import datetime



HANA_SQL_SCRIPT = ''' upsert "NGUMATIMA1"."LINEAGE" ("PACKAGENAME",
"VIEWNAME",
"TARGETCOLUMN",
"MAPPING",
"META_CRT_DT") values(?,?,?,?,?)  WITH PRIMARY KEY
'''

def display_menu():
    print("COMMAND MENU")
    print("1   - Lineage for 1 view")
    print("2 - Entire Package")
    print("3   - Exit program")



view_query = '''
select distinct
package_id,
object_name 

from "_SYS_REPO"."ACTIVE_OBJECT" 
where package_id like 'ILMN.P2D%'
and object_name like '%_QV'
'''


d = {'PACKAGENAME': [1, 302], 'VIEWNAME': [3, 4],'TARGETCOLUMN': [3, 4],'MAPPING':['3', '4'],'META_CRT_DT':[datetime.datetime.now(),datetime.datetime.now()]}

df = pd.DataFrame(data=d)

print(df)

cc = hana_ml.ConnectionContext('analyticsqas.illumina.com',30041,'ngumatima1','NGqas#2597')
hana_cur = cc.connection.cursor()

# for row_count in range(0, df.shape[0], 1):
#     chunk = df.iloc[row_count: row_count+1].values.tolist()
#     tuple_of_tuples = list(tuple(x) for x in chunk)
#     hana_cur.executemany(HANA_SQL_SCRIPT, tuple_of_tuples)
# chunk = df.iloc[0: 1000].values.tolist()
# tuple_of_tuples = list(tuple(x) for x in chunk)
# hana_cur.executemany(HANA_SQL_SCRIPT, tuple_of_tuples)
# hana_cur.close()
hdf = cc.sql(view_query)
df_views = hdf.collect()

for row in df_views.itertuples(index=True, name='Pandas'):
    print( getattr(row, 'PACKAGE_ID'),'--',getattr(row, 'OBJECT_NAME'))

print('hi')

if __name__ == "__main__":
    display_menu()
    while True:
        command = input("\nCommand: ")
        if command=='1':
            packageName = str(input('Enter HANA Package Name: '))
            packageName = packageName.replace(' ', '')
            viewName = str(input('Enter HANA View Name: '))
            viewName = viewName.replace(' ', '')
            viewPath = packageName + '/' + viewName
        elif command=='3':
            sys.exit(0)
        elif command=='2':
            hdf = cc.sql(view_query)
            df_views = hdf.collect()
            for row in df_views.itertuples(index=True, name='Pandas'):
                packageName=getattr(row, 'PACKAGE_ID')
                viewName=getattr(row, 'OBJECT_NAME')
                viewPath = packageName + '/' + viewName


