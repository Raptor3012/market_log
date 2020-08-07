from flask import Flask, url_for, render_template, request, g, abort, flash
from flask_wtf import FlaskForm
from wtforms import TextField
from wtforms.validators import Required
import sqlite3
import os
from geolite2 import geolite2
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

app = Flask(__name__)
app.config.from_object(__name__)

app.config.update(dict(
    CSRF_ENABLED = True,
    DEBUG=True,
    SECRET_KEY='development key'
))
app.config.from_envvar('FLASKR_SETTINGS', silent=True)

class FormSeven(FlaskForm):
        pole1 = TextField('От',validators=[Required()],default="2018-08-00 00:00:00")
        pole2 = TextField('До',validators=[Required()],default="2018-08-15 00:00:00")

class FormSix(FlaskForm):
        pole1 = TextField('От',validators=[Required()],default="2018-08-00 00:00:00")
        pole2 = TextField('До',validators=[Required()],default="2018-08-15 00:00:00")


@app.route('/')
def index():
    return render_template("index.html")

@app.route('/one')
def one():
    engine = create_engine("sqlite:///data/DB_all_to_the_bottom.db", convert_unicode=True)
    result = engine.execute('select * from logs')
    data_logs_df = pd.DataFrame(result,columns= ['index','date','ip_addres','url','country'])    
    result = data_logs_df.groupby(['country']).count().sort_values(by=['date'],ascending=False)[:6].index
    return render_template("one.html",
                            result = result)

@app.route('/two')
def two():
    engine = create_engine("sqlite:///data/DB_all_to_the_bottom.db", convert_unicode=True)
    result = engine.execute('select * from logs')
    data_logs_df = pd.DataFrame(result,columns= ['index','date','ip_addres','url','country'])
    true_search = data_logs_df.url.str.contains('fresh_fish')
    result = data_logs_df.iloc[np.where(true_search == True)].groupby(['country']).count().sort_values(by=['date'],ascending=False)[:6].index
    return render_template("two.html",
                            result = result)

@app.route('/three')
def three():    
    return render_template("three.html",
                            result = result)

@app.route('/four')
def four():
    engine = create_engine("sqlite:///data/DB_all_to_the_bottom.db", convert_unicode=True)
    result = engine.execute('select * from logs')
    data_logs_df = pd.DataFrame(result,columns= ['index','date','ip_addres','url','country'])
    data_logs_df.to_csv('data/export_data_logs.csv',index=False)
    data_logs_df = pd.read_csv('data/export_data_logs.csv',index_col='date',parse_dates=True)
    result = int(data_logs_df.resample('H').url.count().max())
    return render_template("four.html",
                            result = result)

@app.route('/five')
def five():
    
    return render_template("five.html",
                            result = result)

@app.route('/six', methods=('GET', 'POST'))
def six():
    form = FormSix()

    count = "Нажмите кнопку"
    if form.validate_on_submit():
        engine = create_engine("sqlite:///data/DB_all_to_the_bottom.db", convert_unicode=True)
        result = engine.execute('select * from pay')
        pay_df = pd.DataFrame(result,columns=['index','cart_id','ip_addres','user_id','date','pay'])
        pay_df =  pay_df.drop(["index"], axis=1)
        pay_df = pay_df.loc[pay_df.pay == False]

        result = engine.execute('select * from logs')
        data_logs_df = pd.DataFrame(result,columns= ['index','date','ip_addres','url','country'])
        data_logs_df =  data_logs_df.drop(["index"], axis=1)
        data1 = form.pole1.data
        data2 = form.pole2.data
        srez_date = data_logs_df.loc[(data_logs_df.date >= data1) &(data_logs_df.date <= data2)]
        count = 0
        for cart_id in pay_df.cart_id.values:
            for val in srez_date.url:
                if (val.find('cart_id=' + str(cart_id))) !=-1:
                    count += 1
                    break

    return render_template("six.html",
                            form = form,
                            count = count)

@app.route('/seven', methods=('GET', 'POST'))
def seven():
    form = FormSeven()

    count = "Нажмите кнопку"
    if form.validate_on_submit():
        engine = create_engine("sqlite:///data/DB_all_to_the_bottom.db", convert_unicode=True)
        result = engine.execute('select * from pay')
        pay_df = pd.DataFrame(result,columns=['index','cart_id','ip_addres','user_id','date','pay'])
        pay_df =  pay_df.drop(["index"], axis=1)
        pay_df = pay_df.loc[pay_df.pay == True]
        result_count = pay_df.groupby(["ip_addres"]).size().reset_index(name='count_action')
        result_count = result_count.loc[result_count.count_action > 1]
        data1 = form.pole1.data
        data2 = form.pole2.data
        srez_date = pay_df.loc[(pay_df.date >= data1) &(pay_df.date <= data2)]
        count = 0
        for val in result_count.ip_addres.values:
            if srez_date.loc[srez_date.ip_addres == val].shape[0] >=2:
                count +=1

    return render_template("seven.html",
                            form = form,
                            count = count)

if __name__ == '__main__':
    app.run(debug=True)