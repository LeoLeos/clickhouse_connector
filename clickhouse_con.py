# -*- coding: utf-8 -*-
"""
@Time: 2022/4/7 14:17
@Auth: LeoLucky(热衷开源的宝藏Boy)
@Project: clickhouse_connector
@File: clickhouse_con.py
@IDE: PyCharm
@Email: 568428443@qq.com
@BlogSite: www.fangzengye.com
@motto: 学而知不足&写出简洁,易懂的代码是我们共同的追求
@Version:
@Desc:clickhouse数据库连接器
"""

from clickhouse_driver import Client
import pandas as pd
import datetime
from pandahouse import read_clickhouse
import yaml
import os

class ClickhouseConnector(object):
    """
    clickhouse数据库连接类
    """
    def __init__(self, server_name: str, dev: bool = False):
        base_dir = os.path.abspath(os.path.dirname(__file__))
        if dev:
            server_info = self.__read_database_info(base_dir + "/db_name_dev.yaml")[server_name]
        else:
            server_info = self.__read_database_info(base_dir + "/db_name_prod.yaml")[server_name]
        self.host = server_info["host"]
        self.port = server_info["port"]
        self.username = server_info["username"]
        self.password = server_info["password"]
        self.client = Client(host=self.host, port=self.port, user=self.username, password=self.password, compression=True)

    @staticmethod
    def __read_database_info(config_path):
        """
        读取数据库配置信息
        :param config_path:
        :return:
        """
        with open(config_path, 'r', encoding='utf-8') as fp:
            cont = fp.read()
        return yaml.safe_load(cont)

    @staticmethod
    def generate_ck_ddl(insert_df, columns_dict=None):
        """
        :param insert_df: 写入数据
        :param columns_dict: 字段解释映射
        :return:
        """
        ck_ddl = []
        # 常用的三种类型, string, float, int ,time
        for col, d_type in dict(insert_df.dtypes).items():
            if "object" in str(d_type) or "datetime" in str(d_type):
                col_type = "String"
                insert_df[col] = insert_df[col].astype(str)
            elif "float" in str(d_type):
                col_type = "Float64"
                insert_df[col] = insert_df[col].astype(float)
            elif "int" in str(d_type):
                col_type = "Int64"
                insert_df[col] = insert_df[col].astype(int)
            else:
                raise Exception(f"{d_type}无法识别")
            # 组装字段类型
            if columns_dict:
                comment = columns_dict.get(col, "")
            else:
                comment = ""
            one_filed = f"`{col}` {col_type}"
            one_filed = one_filed + f" COMMENT '{comment}'" if comment else one_filed
            # 汇总字段
            ck_ddl.append(one_filed)
        return ','.join(ck_ddl), insert_df

    def ck_show_tables(self, db_name:str):
        sql = f'show tables from {db_name}'
        answer = self.client.execute(sql)
        return answer

    def ck_show_database(self):
        sql = f'SHOW DATABASES'
        answer = self.client.execute(sql)
        return answer

    def create_database(self, database):
        sql = f"CREATE DATABASE IF NOT EXISTS {database} ENGINE = Ordinary"
        answer = self.client.execute(sql)
        return answer

    def ck_create_table(self, ddl_sql):
        """
        创建table
        :param ddl_sql:
        :return:
        """
        try:
            self.client.execute(ddl_sql)
            return True
        except Exception as create_err:
            raise create_err

    def ck_execute_sql(self, sql):
        """
        执行sql
        :param sql:
        :return:
        """
        try:
            answer = self.client.execute(sql)
            return answer
        except Exception as create_err:
            raise create_err

    def ck_drop_table(self, db_name: str, sheet_name: str):
        """
        删除表
        :param db_name:
        :param sheet_name:
        :return:
        """
        sql = f'DROP TABLE IF EXISTS {db_name}.{sheet_name}'
        try:
            self.client.execute(sql)
            return True
        except Exception as E:
            raise E

    def ck_clear_table(self, db_name: str, sheet_name: str):
        """
        截断表
        :param db_name:
        :param sheet_name:
        :return:
        """
        sql = f'TRUNCATE TABLE IF EXISTS {db_name}.{sheet_name}'
        try:
            self.client.execute(sql)
            return True
        except Exception as E:
            raise E

    def ck_get_table_ddl(self, database_name: str, sheet_name: str):
        """
        显示ddl
        :param database_name :
        :param sheet_name :
        """
        # 用 df 的列生成 ddl
        sql = f'SHOW CREATE table {database_name}.{sheet_name}'
        answer = self.client.execute(sql)
        ck_ddl = answer[0][0]
        return ck_ddl

    def ck_get_table_data_numbers(self, database_name: str, sheet_name: str):
        """
        获取表行数
        :param database_name:
        :param sheet_name:
        :return:
        """
        sql = f'SELECT COUNT(id) FROM {database_name}.{sheet_name}'
        answer = self.client.execute(sql)[0][0]
        return answer

    @staticmethod
    def d_type_change(d_type):
        if "object" in str(d_type):
            return ""
        elif "float" in str(d_type):
            return 0.0
        elif "int" in str(d_type):
            return 0
        else:
            return ""

    def save(self, df, db_name: str, sheet_name: str, if_exist="replace", columns_dict=None):
        """
        :param df : DataFrame
        :param sheet_name : sheet_name
        :param if_exist ：append or replace, default replace,不等于replace，就是append
        :param columns_dict comment注释对照
        """
        insert_df = df.copy()
        # 按对应列的格式填充数据
        insert_df.fillna(
            {col: self.d_type_change(d_type) for col, d_type in dict(insert_df.dtypes).items()}, inplace=True
        )
        if "update_time" not in list(insert_df.columns):
            insert_df['update_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if "id" in insert_df.columns:
            del insert_df['id']
        new_ddl, insert_df = self.generate_ck_ddl(insert_df, columns_dict)
        if if_exist == "replace":
            # 执行一个删表
            self.ck_drop_table(db_name, sheet_name)
        # 执行一个建表
        table_ddl = f'''
        CREATE TABLE IF NOT EXISTS {db_name}.{sheet_name} (`id` UInt64, {new_ddl}) 
        ENGINE = MergeTree() ORDER BY (id);
        '''
        self.ck_create_table(table_ddl)
        # 增加id
        if "id" in insert_df.columns:
            del insert_df['id']
        exist_max_id = 0 if if_exist == "replace" else self.ck_get_table_data_numbers(sheet_name=sheet_name)
        insert_df['id'] = [one_num + 1 + exist_max_id for one_num in range(len(insert_df.index))]
        # 数据总是以尽量大的batch进行写入，如每次写入100,000行。
        cols = ','.join(map(lambda m_x: f"`{m_x}`", list(insert_df.columns)))
        data_dict_list = insert_df.to_dict('records')
        data_list = [list(one_item.values()) for one_item in data_dict_list]
        # data_list = data_dict_list
        group_limit = 1000000
        insert_num = int(len(insert_df.index) / group_limit) + 1
        for one_num in range(insert_num):
            now_data_list = data_list[(one_num * group_limit): (one_num * group_limit + group_limit)]
            if now_data_list:
                date_str = [tuple(i) for i in now_data_list]
                insert_sql = f"INSERT INTO {db_name}.{sheet_name} ({cols}) VALUES "+str(date_str)[1:-1]
                self.client.execute(insert_sql)

    def query(self, ck_sql: str):
        """
        :param ck_sql : clickhouse的sql语句
        :param columns : list 返回的df的列名,为空则没有列名
        """
        answer = self.client.execute(ck_sql, with_column_types=True)
        data_list = answer[0]
        answer_df = pd.DataFrame(data_list)
        columns_result_list = [one_item[0] for one_item in answer[1]]
        if columns_result_list and data_list:
            answer_df.columns = columns_result_list
        return answer_df


    def ck_do_sql(self, ddl_sql):
        """
        执行sql
        :param ddl_sql:
        :return:
        """
        try:
            self.client.execute(ddl_sql)
            return True
        except Exception as create_err:
            raise create_err
