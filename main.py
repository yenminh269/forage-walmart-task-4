import csv
import sqlite3

conn = sqlite3.connect('shipment_database.db')
cursor = conn.cursor()
print("Connected to database sucessfully")

product_cache = {} #keep track of products to avoid duplicates

#insert spreadsheet 0 into the database
with open('data/shipping_data_0.csv', newline='') as csvfile:
    spamreader = csv.reader(csvfile)
    next(spamreader) #skip header
    for row in spamreader:
        origin, dest, product_name, on_time, product_quantity, driver_identification = row
        quantity = int(product_quantity)

        if product_name not in product_cache:
            cursor.execute("INSERT INTO product(name) VALUES (?)", (product_name,))
            product_id = cursor.lastrowid
            product_cache[product_name] = product_id
        else:
            product_id = product_cache[product_name]

         # insert into shipment table   
        cursor.execute("INSERT INTO shipment(product_id, quantity, origin, destination) " \
        "VALUES(?,?,?,?)", (product_id, quantity, origin, dest))

shipment_product = {}

#count products per shipment
with open('data/shipping_data_1.csv', newline='', encoding='utf-8') as csvfile:
    spamreader = csv.reader(csvfile)
    next(spamreader) #skip header
    for row in spamreader:
        shipment_id, product_name, on_time = row
        if shipment_id not in shipment_product:
            shipment_product[shipment_id] = {}
        if product_name not in shipment_product[shipment_id]:
            shipment_product[shipment_id][product_name] = 1
        else:
            shipment_product[shipment_id][product_name] += 1

#insert shipments into database
with open('data/shipping_data_2.csv', newline='', encoding='utf-8') as csvfile:
    spamreader = csv.reader(csvfile)
    next(spamreader) 
    for row in spamreader:
        shipment_id, origin_warehouse, dest, driver_id = row
        if shipment_id in shipment_product:
            products_dict = shipment_product[shipment_id]
            for product_name, quantity in products_dict.items():
                cursor.execute("SELECT id FROM product WHERE name = ?", (product_name,))
                product_id_row = cursor.fetchone()
                if product_id_row is None:
                    continue
                product_id = product_id_row[0]
                cursor.execute("INSERT INTO shipment(product_id, quantity, origin, destination) VALUES(?,?,?,?)", 
                               (product_id, quantity, origin_warehouse, dest))   
conn.commit()
conn.close()
print("Data inserted successfully")