from database.mysql import MYSQL
from pathlib import Path


def ingest_data_from_csv(csv_path:Path) -> None:
    # 数据库连接
    mysql = MYSQL()
    # 获取数据
    csv_path :str = str(csv_path)
    table_name :str = csv_path.split('\\')[-1].split('.')[0]
    # 数据入库
    mysql.load_data_local_infile(csv_path=csv_path, table_name=table_name, ignore_lines=2)



test_csv_path = Path(r'test_data\stock-trading-data-2024-09-28N\bj836720.csv')
ingest_data_from_csv(csv_path=test_csv_path)