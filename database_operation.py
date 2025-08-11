from database.mysql import MYSQL
from pathlib import Path


def ingest_data_from_csv(csv_path:Path) -> None:
    # 数据库连接
    mysql = MYSQL()
    # 获取数据
    csv_path :str = str(csv_path)
    table_name :str = csv_path.split('\\')[-1].split('.')[0]
    # 数据入库
    # 中文编码需要指定为gbk， 默认为utf-8
    # 忽略csv文件前两行，xbx文件第一行为广子，第二行为表头
    mysql.load_data_local_infile(csv_path=csv_path, table_name=table_name, ignore_lines=2, decoder='gbk')


# 遍历文件夹中的csv文件
if __name__ == '__main__':
    # 获取文件夹中的所有csv文件
    csv_files = list(Path(r'test_data\stock-trading-data-2024-09-28N').glob('*.csv'))
    # 遍历csv文件
    for csv_file in csv_files:
        # 数据入库
        ingest_data_from_csv(csv_path=csv_file)
