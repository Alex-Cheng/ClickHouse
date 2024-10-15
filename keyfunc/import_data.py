import os
import re
from clickhouse_driver import Client

# 设置ClickHouse客户端
client = Client(host='localhost', port='9000', user='default', password='', database='default')

print(client.execute('''show tables'''))

# 遍历文件夹
folder_path = '/Users/alexcheng/Downloads/ch_pg_eval'
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):        
        # 提取表名，去除数字和后缀
        table_name = re.sub(r'_\d+\.csv', '', filename)

        # 创建表SQL
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name}  ENGINE = MergeTree() 
        ORDER BY () AS
        select * from file('ch_pg_eval/{filename}')
        """
        print(create_table_sql)
        
        # 执行创建表SQL
        client.execute(create_table_sql)
        print(f"Table {table_name} created and data inserted.")

print("All CSV files have been processed.")
