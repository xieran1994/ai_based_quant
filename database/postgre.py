"""PostgreSQL数据库操作模块"""

import psycopg2
from psycopg2 import sql, Error
from typing import Any, List, Dict, Optional, Tuple
import pandas as pd
from loguru import logger


class PostgreSQL:
    """PostgreSQL数据库操作类"""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "postgres",
        user: str = "postgres",
        password: str = "postgres",
    ):
        """
        初始化PostgreSQL连接

        Args:
            host: 主机地址
            port: 端口号
            database: 数据库名
            user: 用户名
            password: 密码
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None

    def connect(self) -> bool:
        """
        连接到PostgreSQL数据库

        Returns:
            连接是否成功
        """
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            self.cursor = self.connection.cursor()
            logger.info(
                f"成功连接到PostgreSQL: {self.user}@{self.host}:{self.port}/{self.database}"
            )
            return True
        except Error as e:
            logger.error(f"连接PostgreSQL失败: {e}")
            return False

    def disconnect(self) -> None:
        """断开数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭")

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> bool:
        """
        执行数据库查询（用于INSERT, UPDATE, DELETE）

        Args:
            query: SQL查询语句
            params: 查询参数

        Returns:
            执行是否成功
        """
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.connection.commit()
            logger.info(f"查询执行成功: {self.cursor.rowcount} 行受影响")
            return True
        except Error as e:
            self.connection.rollback()
            logger.error(f"执行查询失败: {e}")
            return False

    def fetch_query(
        self, query: str, params: Optional[Tuple] = None
    ) -> Optional[List[Tuple]]:
        """
        执行数据库查询（用于SELECT）

        Args:
            query: SQL查询语句
            params: 查询参数

        Returns:
            查询结果列表
        """
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            results = self.cursor.fetchall()
            return results
        except Error as e:
            logger.error(f"执行查询失败: {e}")
            return None

    def insert_data(self, table: str, data: Dict[str, Any]) -> bool:
        """
        向表中插入一条数据

        Args:
            table: 表名
            data: 数据字典 {列名: 值}

        Returns:
            插入是否成功
        """
        try:
            columns = data.keys()
            values = [data[col] for col in columns]

            placeholders = ", ".join(["%s"] * len(columns))
            query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(placeholders),
            )

            return self.execute_query(query.as_string(self.connection), tuple(values))
        except Error as e:
            logger.error(f"插入数据失败: {e}")
            return False

    def insert_many(self, table: str, data_list: List[Dict[str, Any]]) -> bool:
        """
        批量插入数据

        Args:
            table: 表名
            data_list: 数据字典列表

        Returns:
            插入是否成功
        """
        if not data_list:
            return False

        try:
            columns = data_list[0].keys()
            placeholders = ", ".join(["%s"] * len(columns))

            query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(table),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(placeholders),
            )

            for data in data_list:
                values = [data[col] for col in columns]
                self.cursor.execute(query.as_string(self.connection), tuple(values))

            self.connection.commit()
            logger.info(f"批量插入成功: {len(data_list)} 条数据")
            return True
        except Error as e:
            self.connection.rollback()
            logger.error(f"批量插入失败: {e}")
            return False

    def read_data(
        self,
        table: str,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        params: Optional[Tuple] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> Optional[List[Tuple]]:
        """
        读取表中的数据

        Args:
            table: 表名
            columns: 列名列表（默认为*）
            where: WHERE子句（不含WHERE关键字）
            params: WHERE子句参数
            limit: 限制返回行数
            order_by: 排序子句（不含ORDER BY关键字）

        Returns:
            查询结果列表
        """
        try:
            col_str = ", ".join(columns) if columns else "*"
            query = f"SELECT {col_str} FROM {table}"

            if where:
                query += f" WHERE {where}"

            if order_by:
                query += f" ORDER BY {order_by}"

            if limit:
                query += f" LIMIT {limit}"

            return self.fetch_query(query, params)
        except Error as e:
            logger.error(f"读取数据失败: {e}")
            return None

    def read_dataframe(
        self,
        table: str,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        params: Optional[Tuple] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """
        读取表中的数据为DataFrame

        Args:
            table: 表名
            columns: 列名列表
            where: WHERE子句
            params: WHERE子句参数
            limit: 限制返回行数
            order_by: 排序子句

        Returns:
            Pandas DataFrame或None
        """
        try:
            col_str = ", ".join(columns) if columns else "*"
            query = f"SELECT {col_str} FROM {table}"

            if where:
                query += f" WHERE {where}"

            if order_by:
                query += f" ORDER BY {order_by}"

            if limit:
                query += f" LIMIT {limit}"

            return pd.read_sql(query, self.connection, params=params)
        except Error as e:
            logger.error(f"读取DataFrame失败: {e}")
            return None

    def update_data(
        self,
        table: str,
        data: Dict[str, Any],
        where: str,
        params: Optional[Tuple] = None,
    ) -> bool:
        """
        更新表中的数据

        Args:
            table: 表名
            data: 要更新的数据字典 {列名: 新值}
            where: WHERE子句（不含WHERE关键字）
            params: WHERE子句参数

        Returns:
            更新是否成功
        """
        try:
            set_clause = ", ".join([f"{col} = %s" for col in data.keys()])
            query = f"UPDATE {table} SET {set_clause} WHERE {where}"

            values = list(data.values())
            if params:
                values.extend(params)

            return self.execute_query(query, tuple(values))
        except Error as e:
            logger.error(f"更新数据失败: {e}")
            return False

    def update_many(
        self,
        table: str,
        data_list: List[Dict[str, Any]],
        where: str,
        params_list: List[Tuple],
    ) -> bool:
        """
        批量更新数据

        Args:
            table: 表名
            data_list: 要更新的数据字典列表
            where: WHERE子句
            params_list: WHERE子句参数列表

        Returns:
            更新是否成功
        """
        if not data_list or len(data_list) != len(params_list):
            return False

        try:
            for data, params in zip(data_list, params_list):
                self.update_data(table, data, where, params)
            logger.info(f"批量更新成功: {len(data_list)} 条数据")
            return True
        except Error as e:
            logger.error(f"批量更新失败: {e}")
            return False

    def delete_data(
        self, table: str, where: Optional[str] = None, params: Optional[Tuple] = None
    ) -> bool:
        """
        删除表中的数据

        Args:
            table: 表名
            where: WHERE子句（不含WHERE关键字）
            params: WHERE子句参数

        Returns:
            删除是否成功
        """
        try:
            query = f"DELETE FROM {table}"

            if where:
                query += f" WHERE {where}"
            else:
                logger.warning(f"删除整个表 {table} 的所有数据")

            return self.execute_query(query, params)
        except Error as e:
            logger.error(f"删除数据失败: {e}")
            return False

    def delete_by_id(self, table: str, id_column: str, id_value: Any) -> bool:
        """
        根据ID删除数据

        Args:
            table: 表名
            id_column: ID列名
            id_value: ID值

        Returns:
            删除是否成功
        """
        where = f"{id_column} = %s"
        return self.delete_data(table, where, (id_value,))

    def table_exists(self, table: str) -> bool:
        """
        检查表是否存在

        Args:
            table: 表名

        Returns:
            表是否存在
        """
        query = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = %s
            )
        """
        result = self.fetch_query(query, (table,))
        return result[0][0] if result else False

    def get_row_count(
        self, table: str, where: Optional[str] = None, params: Optional[Tuple] = None
    ) -> Optional[int]:
        """
        获取表中的行数

        Args:
            table: 表名
            where: WHERE子句
            params: WHERE子句参数

        Returns:
            行数
        """
        try:
            query = f"SELECT COUNT(*) FROM {table}"

            if where:
                query += f" WHERE {where}"

            result = self.fetch_query(query, params)
            return result[0][0] if result else 0
        except Error as e:
            logger.error(f"获取行数失败: {e}")
            return None

    def get_columns(self, table: str) -> Optional[List[str]]:
        """
        获取表的所有列名

        Args:
            table: 表名

        Returns:
            列名列表
        """
        try:
            query = """
                SELECT column_name FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """
            results = self.fetch_query(query, (table,))
            return [col[0] for col in results] if results else None
        except Error as e:
            logger.error(f"获取列名失败: {e}")
            return None

    def create_table(self, table: str, schema: Dict[str, str]) -> bool:
        """
        创建表

        Args:
            table: 表名
            schema: 列定义字典 {列名: 数据类型和约束}
                    例如: {'id': 'SERIAL PRIMARY KEY', 'name': 'VARCHAR(100)'}

        Returns:
            创建是否成功
        """
        try:
            columns_def = ", ".join([f"{col} {dtype}" for col, dtype in schema.items()])
            query = f"CREATE TABLE IF NOT EXISTS {table} ({columns_def})"
            return self.execute_query(query)
        except Error as e:
            logger.error(f"创建表失败: {e}")
            return False

    def drop_table(self, table: str, if_exists: bool = True) -> bool:
        """
        删除表

        Args:
            table: 表名
            if_exists: 如果表存在才删除

        Returns:
            删除是否成功
        """
        try:
            exists_clause = "IF EXISTS" if if_exists else ""
            query = f"DROP TABLE {exists_clause} {table}"
            return self.execute_query(query)
        except Error as e:
            logger.error(f"删除表失败: {e}")
            return False
