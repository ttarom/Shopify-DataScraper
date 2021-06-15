import pandas as pd
import psycopg2
import time
import os
from datetime import datetime, timedelta
import shopify

API_KEY = os.getenv('API_KEY')
SECRET = os.environ.get('SECRET')
API_VERSION = '2021-01'

# Shopify credit limit buffers to avoid ERROR 429 Too Many Requests error
credit = 10
# Back fill days to choose from
days = 365
date_fill = pd.date_range(str(datetime.today().date() - timedelta(days=days)), periods=days + 1, freq='d')
print(date_fill)


# For loop on dates
def run():
    print('starting job....')
    start = time.time()
    for i in range(0, len(date_fill) - 1):
        updated_at_min = str(date_fill[i]) + 'T00:00:00-00:00'
        updated_at_max = str(date_fill[i + 1]) + 'T23:59:59-00:00'
        iter_all_orders(updated_at_min, updated_at_max)
        out = read_data(updated_at_min, updated_at_max)
        df = data_to_df(out)
        load_data_sql(df, updated_at_min, updated_at_max)
    end = time.time()
    print('execution time of script: ', str(timedelta(seconds=(end - start))))


# Paginating data for scraping
def iter_all_orders(updated_at_min, updated_at_max):
    since_id = '0'
    limit = 250
    updated_max_save = updated_at_max
    get_next_page = True
    while get_next_page:
        while (shopify.Limits.credit_left()) <= credit:
            print('Warning! low on credits. Credits left: ', shopify.Limits.credit_left())
            time.sleep(5.0)
        orders = shopify.Order.find(updated_at_min=updated_at_min, updated_at_max=updated_at_max, since_id=since_id,
                                    limit=limit, status='any')
        for order in orders:
            yield order
            print(order.id, order.created_at, order.updated_at)
        updated_at_min = updated_at_min
        updated_at_max = updated_max_save
        if not orders:
            since_id = str(float(since_id) + 1)[0:12]
        else:
            since_id = orders[-1].id
        if len(orders) < limit:
            get_next_page = False

    while (shopify.Limits.credit_left()) <= credit:
        print('Warning! low on credits. Credits left: ', shopify.Limits.credit_left())
        time.sleep(5.0)


# Parsing request JSON and appending them
def read_data(updated_at_min, updated_at_max):
    print('reading data')
    # Set environmental variables
    shop_url = "https://yourstore.myshopify.com/admin/api/{}".format(API_VERSION)
    shopify.ShopifyResource.set_site(shop_url)
    shopify.ShopifyResource.set_user(API_KEY)
    shopify.ShopifyResource.set_password(SECRET)

    while (shopify.Limits.credit_left()) <= credit:
        print('Warning! low on credits. Credits left: ', shopify.Limits.credit_left())
        time.sleep(5.0)

    items = []
    order_id = 0

    # for loop on pagination function iter_all_orders
    for order in iter_all_orders(updated_at_min, updated_at_max):
        order_id = order.id
        externalordernumber = order.name
        created_at = order.processed_at[:10]
        updated_at = order.updated_at[:10]
        shipping = sum([float(ship.price) for ship in order.shipping_lines])
        for line_items in order.line_items:
            line_item_ids = line_items.id
            title = line_items.title
            vendor = line_items.vendor
            sku = line_items.sku
            price = line_items.price
            ordered_item_quantity = line_items.quantity
            gross_sales = (float(line_items.price) * float(line_items.quantity))
            net_sales = line_items.price
            taxes = sum([float(tax_lines.price) for tax_lines in line_items.tax_lines])
            discounts = -sum([float(discounts.amount) for discounts in line_items.discount_allocations])
            items.append(
                [title, vendor, sku, created_at, updated_at, price, order_id, taxes, gross_sales, discounts, net_sales,
                 ordered_item_quantity, line_item_ids, shipping, externalordernumber])
    # Checks if any order was inserted
    if order_id == 0:
        print('empty page - skipping')
        pass
    else:
        line_items = pd.DataFrame(items)
        out = line_items.rename(columns={0: 'title', 1: 'vendor', 2: 'sku', 3: 'created_at', 4: 'updated_at',
                                                5: 'price', 6: 'order_id', 7: 'taxes', 8: 'gross_sales', 9: 'discounts',
                                                10: 'net_sales', 11: 'ordered_item_quantity', 12: 'line_item_ids',
                                                13: 'shipping', 14: 'externalordernumber'})

        print(out, out.dtypes)

        return out


# Turning list into a pandas DataFrame
def data_to_df(out):
    # Check if out returned any data:
    if out is None:
        print('empty dataframe - skipping')
        pass
    else:
        df = pd.DataFrame(out)
        df = df.drop_duplicates()
        df = df[['title', 'vendor', 'sku', 'created_at', 'updated_at', 'price', 'order_id',
                 'gross_sales', 'discounts', 'net_sales', 'taxes', 'ordered_item_quantity',
                 'shipping', 'externalordernumber']]
        df = df.sort_values(by=['created_at', 'order_id', 'sku'])
        print(df.tail())
        print(df.dtypes)
        print('number of rows and columns recorded in the data: ', df.shape)

        return df


# Inserting data into PostgreSQL
def load_data_sql(df, updated_at_min, updated_at_max):
    connection = 0
    # Check if df returned any data:
    if df is None:
        pass
        print('skipping insert to SQL')
        time.sleep(1.0)
    else:
        try:
            # Connect to PostgreSQL DBMS:
            connection = psycopg2.connect(user=os.getenv('USER'),
                                          password=os.environ.get('PASSWORD'),
                                          host="host",
                                          port="5432",
                                          database="db")
            cursor = connection.cursor()
            truncate_query = "DELETE FROM table WHERE updated_at BETWEEN" + ("'") + updated_at_min[:10] + ("'") + (
                                                                 " AND ") + ("'") + updated_at_max[:10] + ("'")
            cursor.execute(truncate_query)
            connection.commit()
            print("deleted raws greater than insert date to avoid duplicates before new insert batch")

            # Turning dataframe into tuples format to match PostgreSQL syntax
            records_to_insert = list(df.itertuples(index=False, name=None))

            print('begin insert of data to table')

            cursor = connection.cursor()
            args_strings = ','.join(
                cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x).decode('utf-8')
                for x in records_to_insert)
            sql_insert_query = "INSERT INTO table VALUES " + args_strings
            cursor.execute(sql_insert_query)
            connection.commit()
            print(cursor.rowcount, "Records inserted successfully into the table")

        # Prints a log of error if caught
        except (Exception, psycopg2.Error) as error:
            print("Failed inserting record into table {}".format(error))

        finally:
            # closing database connection.
            if connection:
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed.")
