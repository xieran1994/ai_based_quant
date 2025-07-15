import pymysql
import sys
import csv

from typing import Union

from config.mysql_config import MYSQL_CONFIG
from loguru import logger
# from config.logger import Logger

# Logger.set_name(new_name = 'MYSQL')
class MYSQL:
    _DEFAULT_CONFIG = MYSQL_CONFIG
    def __init__(self, **kwargs):
        for name, default in self._DEFAULT_CONFIG.items():
            setattr(self, name, kwargs.get(name, default))
        
        self.mysql_connection = self._connection()
    
    # MYSQL类可以输入database参数
    @property
    def database(self):
        return self._database
    
    @database.setter
    def database(self, value):
        self._database = value

    def _connection(self) -> None:
        '''
        :return: None
        '''
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                local_infile=self.local_infile
            )
            logger.info(f"数据库连接成功")
            return conn
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            sys.exit(1)

    def load_data_local_infile(self, csv_path: str, table_name: str, delimiter: str = ',', ignore_lines: int = 1) -> None:
        '''
        :param csv_path: csv文件路径
        :param table_name: 表名
        :return: None
        '''

        if not csv_path:
            logger.error(f"{csv_path} 不存在")
            return

        escaped_path = pymysql.converters.escape_string(csv_path)
        escaped_table = pymysql.converters.escape_string(table_name)
        escaped_delimiter = pymysql.converters.escape_string(delimiter)

        try:
            with self.mysql_connection.cursor() as cursor:
                cursor.execute(f"SHOW TABLES LIKE '{escaped_table}'")
                if not cursor.fetchone():
                    logger.error(f"表 {table_name} 不存在")
                    logger.info(f"创建表 {table_name}")
                    headers = self._get_csv_header(csv_path)
                    self.create_table(cursor, table_name, headers) 
        except Exception as e:
            logger.error(f"查询表 {table_name} 失败: {e}")
            sys.exit(1)

        load_data_sql = f"""
            LOAD DATA LOCAL INFILE '{escaped_path}'
            INTO TABLE {escaped_table}
            CHARACTER SET gbk
            FIELDS TERMINATED BY '{escaped_delimiter}'
            OPTIONALLY ENCLOSED BY '"'
            LINES TERMINATED BY '\r\n'
            IGNORE {ignore_lines} LINES
            """
        # with self.mysql_connection.cursor() as cursor:
        #     cursor.execute(f"SET GLOBAL local_infile = 1")
        #     self.mysql_connection.commit()
        try:
            with self.mysql_connection.cursor() as cursor:
                cursor.execute(load_data_sql)
                self.mysql_connection.commit()
            logger.info(f"成功导入数据到表 {table_name},共 {cursor.rowcount} 行")
        except Exception as e:
            logger.error(f"导入数据到表 {table_name} 失败: {e}")
        finally:
            self.mysql_connection.close()

    def create_table(self, cursor: pymysql.cursors.Cursor, table_name: str, headers: zip) -> None:
        '''
        :param table_name: 表名
        :return: None
        '''

        cols = [f"{header} {typ}" for header, typ in headers]
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(cols)})"

        try:
            cursor.execute(create_table_sql)
            self.mysql_connection.commit()
            logger.info(f"成功创建表 {table_name}")
        except Exception as e:
            logger.error(f"创建表 {table_name} 失败: {e}")

    def _get_csv_header(self, csv_path: str) -> zip:
        '''
        :param csv_path: csv文件路径
        :return: zip
        '''

        with open(csv_path, 'r') as f:
            lines = f.readlines()[1:] # xbx的csv第一行有广子，忽略第一行
            reader = csv.reader(lines)
            headers = next(reader)

            sample_row = next(reader)
            col_types = []
            for value in sample_row:
                if value.isdigit():
                    col_types.append('INT')
                else:
                    col_types.append('VARCHAR(255)')

            return zip(headers, col_types)
