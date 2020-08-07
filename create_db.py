from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table, Column
from sqlalchemy import Integer, String, DateTime, Boolean
import os
import pandas as pd

def Create_DataBase():
    '''Возвращает metadata в которой хранится структура всех таблиц'''

    if os.path.exists("data/DB_all_to_the_bottom.db"):os.remove("data/DB_all_to_the_bottom.db")
    engine = create_engine("sqlite:///data/DB_all_to_the_bottom.db")
    metadata = MetaData()
    log_table = Table('logs', metadata,
                    Column('logs_index', Integer,primary_key=True),
                    Column('date', String),
                    Column('ip_addres', String),
                    Column('url', String),
                    Column('country', String)
                )
    cart_table = Table('cart', metadata,
                    Column('cart_index', Integer,primary_key=True),
                    Column('cart_id', Integer),
                    Column('goods_id', Integer),
                    Column('amount', Integer)
                )
    pay_table = Table('pay', metadata,
                    Column('pay_index', Integer,primary_key=True),
                    Column('cart_id', Integer),
                    Column('ip_addres', String),
                    Column('user_id', Integer),
                    Column('date', String),
                    Column('success_pay', Boolean)
                )
    goods_category_table = Table('goods_category',metadata,
                    Column('goods_category_index', Integer,primary_key=True),
                    Column('category_id', Integer),
                    Column('name_category', String)
                )
    goods_table = Table('goods',metadata,
                    Column('goods_index', Integer,primary_key=True),
                    Column('goods_id', Integer),
                    Column('name_goods', String)
                )
    goods_and_category_table = Table('goods_and_category',metadata,
                    Column('goods_and_category_index', Integer,primary_key=True),
                    Column('category_id', Integer),
                    Column('goods_id', Integer)
                )
    metadata.create_all(engine)

    return metadata

