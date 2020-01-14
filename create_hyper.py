import numpy as np
import pandas as pd
from tableauhyperapi import HyperProcess, Connection, TableDefinition, SqlType, Telemetry, Inserter, CreateMode, TableName

#generate df and populate table rows
df = pd.DataFrame({'letter': list('abcdefghijklmnopqrstuvwxyz'),
                   'value': np.random.randint(1,100, size=26)})
table_rows = []
for row in df.itertuples(index=False, name=None):
    table_rows.append(row)

#set up schema
table_dict = {'letter': SqlType.text(), 'value': SqlType.double()}
table_def = []
for k, v in table_dict.items():
    table_def.append(TableDefinition.Column(k,v))

#populate hyper file
with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
    with Connection(hyper.endpoint, 'letter_file.hyper', CreateMode.CREATE_AND_REPLACE) as connection:
        #create schema
        connection.catalog.create_schema('Extract')
        
        #create table
        table = TableDefinition(TableName('Extract', 'Extract'), table_def)
        connection.catalog.create_table(table)
        
        #insert data into table
        with Inserter(connection, table) as inserter:
            for i in table_rows:
                inserter.add_rows([i])
            inserter.execute()
