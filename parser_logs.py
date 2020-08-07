import pandas as pd
import numpy as np

def run_parser(data_logs):
    '''
    Режет данные и помещает в pandas датафреймы

    На вход принимает:
        data_logs:  сформированые логи

    Возвращает pandas датафреймы:
        goods_category_df: категории продуктов
        goods_df: названия продуктов и их категории + в индексном формате
        cart_df:  id корзины, добавленный продукт и его кол-ве
        pay_df: id корзины, ip addres купившего, id купившего, дата покупки, подтверждение оплаты
    '''
    goods_category = []
    goods_c_and_goods = []
    cart = []
    pay = []
    success_pay = []
    data_logs_sort = data_logs.sort_values(by=['ip_addres','date'])

    #Извлекаем уникальные url и удаляем из них общую часть
    unique_url = pd.unique(data_logs['url']).tolist()
    i = 0
    for val in unique_url:
        unique_url[i] =  val.replace('https://all_to_the_bottom.com/',"")
        i += 1
    i = -1
    #Собственно сам парсер
    for val in unique_url:
        i +=1
        #Находим строку обозначающую добавление товара в корзину
        #И помещаем данные в список
        if(val.find('cart?') != -1):
            val_split =  val[val.find('?')+1:].split('&')
            goods_id = int(val_split[0][val_split[0].find('=')+1:])
            amount = int(val_split[1][val_split[1].find('=')+1:])
            cart_id = int(val_split[2][val_split[2].find('=')+1:])
            cart.append([cart_id,goods_id,amount])
            continue

        #Находим строку обозначающую попытку оплаты корзины
        #И помещаем данные в список
        if(val.find('pay?') != -1):
            val_split =  val[val.find('?')+1:].split('&')
            user_id = int(val_split[0][val_split[0].find('=')+1:])
            cart_id = int(val_split[1][val_split[1].find('=')+1:])
            date = str(data_logs.loc[data_logs.url == 'https://all_to_the_bottom.com/pay?user_id=' + str(user_id) + '&cart_id=' + str(cart_id)].date.values[0])
            ip_addres = str(data_logs.loc[data_logs.url == 'https://all_to_the_bottom.com/pay?user_id=' + str(user_id) + '&cart_id=' + str(cart_id)].ip_addres.values[0])
            sp = 0
            pay.append([cart_id,ip_addres,user_id,date,sp])
            continue

        #Находим строку обозначающую подтверждение оплаты
        #И помещаем данные в список
        if(val.find('success_pay_') != -1):
            val_split =  val.split('_')
            success_pay.append(int(val_split[2][:-1]))
            continue

        #Обрабатываем строки отвечающие за категории и продукты
        val_categ = val[:val.find('/')]
        if len(val_categ) != 0: goods_category.append(val_categ)

        val_goods = val[val.find('/')+1 :-1]
        if len(val_goods) != 0:
            val_g_c = val[:-1]
            goods_c_and_goods.append(val_g_c)

    i = 0
    for val in goods_c_and_goods:
        goods_c_and_goods[i] =  val.split('/')
        i += 1
    #Создаем датафреймы из найденных данных приводя их к нужному виду и дополняя
    goods_category = np.unique(np.array(goods_category)).tolist()
    goods_category_df = pd.DataFrame(goods_category,columns=["name_category"])
    goods_df = pd.DataFrame(goods_c_and_goods,columns=["name_category","name_goods"])
    goods_df["goods_id"] = 0
    goods_df["category_id"] = 0
    goods_df = goods_df[["category_id","goods_id","name_category","name_goods"]]
    cart_df = pd.DataFrame(cart,columns=["cart_id","goods_id","amount"])

    url_mask = []
    for i in range(goods_df.shape[0]):
        if i < 9:
            url_mask.append('https://all_to_the_bottom.com/cart\Dgoods_id=' + str(i+1) + "\D")
        else:
            url_mask.append('https://all_to_the_bottom.com/cart\Dgoods_id=' + str(i+1))

    for i in range(len(url_mask)):
        true_false_mask = data_logs_sort.url.str.contains(url_mask[i])
        find_str = data_logs_sort.iloc[np.where(true_false_mask == True)[0][0] - 1].url
        find_str = find_str.replace('https://all_to_the_bottom.com/', "").split('/')
        find_category = find_str[0]
        find_goods = find_str[1]
        for j in range(goods_df.shape[0]):
            if (goods_df.name_goods[j] == find_goods) & (goods_df.name_category[j] == find_category):
                goods_df.goods_id[j] = i + 1
                break
            
    for i in range(goods_df.shape[0]):
        for j in range(goods_category_df.shape[0]):
            if goods_df.name_category[i] == goods_category_df.name_category[j]:
                goods_df.category_id[i] = j + 1

    for i in range(len(success_pay)):
        for j in range(len(pay)):
            if pay[j][0] == success_pay[i]:
                pay[j][-1] = 1

    pay_df = pd.DataFrame(pay,columns=["cart_id","ip_addres","user_id",'date',"success_pay"])
    unique_cart_id =  pd.unique(cart_df['cart_id']).tolist()
    flag = False
    for i in range(len(unique_cart_id)):
        flag = False
        for j in range(len(pay)):
            if unique_cart_id[i] in pay[j]:
                flag = True
                break
        if flag == False:
            pay_df.loc[pay_df.shape[0]] = [unique_cart_id[i], '0.0.0.0', 0,'0', 0]

    return goods_category_df, goods_df, cart_df, pay_df