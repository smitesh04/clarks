import pymysql

class DbConfig():

    def __init__(self):
        self.con = pymysql.Connect(host='localhost',
                              user='root',
                              password='actowiz',
                              database='clarks')
        self.cur = self.con.cursor(pymysql.cursors.DictCursor)
        self.data_table = 'data'
        self.store_links_table = 'store_links'

    def check_table_exists(self, table_name):
        query = f"SHOW TABLES LIKE '{table_name}';"
        self.cur.execute(query)
        return self.cur.fetchone() is not None

    def create_data_table(self, data_table):
        if not self.check_table_exists(data_table):

            query = f'''
                CREATE TABLE if not exists `{data_table}` (
                  `id` int NOT NULL AUTO_INCREMENT,
                  `store_no` varchar(255) DEFAULT NULL,
                  `name` varchar(255) DEFAULT NULL,
                  `latitude` varchar(255) DEFAULT NULL,
                  `longitude` varchar(255) DEFAULT NULL,
                  `street` varchar(500) DEFAULT NULL,
                  `city` varchar(50) DEFAULT NULL,
                  `state` varchar(50) DEFAULT NULL,
                  `zip_code` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
                  `county` varchar(50) DEFAULT NULL,
                  `phone` varchar(50) DEFAULT NULL,
                  `open_hours` varchar(500) DEFAULT NULL,
                  `url` varchar(255) DEFAULT NULL,
                  `provider` varchar(50) DEFAULT NULL,
                  `category` text,
                  `updated_date` varchar(255) DEFAULT NULL,
                  `country` tinytext,
                  `status` tinytext,
                  `direction_url` varchar(255) DEFAULT NULL,
                  `pagesave_path` varchar(255) DEFAULT NULL,
                  PRIMARY KEY (`id`),
                  UNIQUE KEY `store_no` (`store_no`)
                )
                '''

            self.cur.execute(query)
            self.con.commit()
            print(f'Table {data_table} has been created! ')

    def insert_data_table(self, item):

        query = f'''
                        INSERT INTO {self.data_table} (store_no, name, latitude, longitude, street, city, state, zip_code, county, phone, open_hours, url, provider, category, updated_date, country, status, direction_url, pagesave_path)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        '''
        data = (
            item["store_no"],
            item["name"],
            item["latitude"],
            item["longitude"],
            item["street"],
            item["city"],
            item["state"],
            item["zip_code"],
            item["county"],
            item["phone"],
            item["open_hours"],
            item["url"],
            item["provider"],
            item["category"],
            item["updated_date"],
            item["country"],
            item["status"],
            item["direction_url"],
            item["pagesave_path"]
        )

        try:
            self.cur.execute(query.format(data_table=self.data_table), data)
            self.con.commit()
            print(item)
        except Exception as e:
            print(e)

    def insert_store_links_table(self, item):
        qr = f'''
            insert into {self.store_links_table}(
                        store_id, 
                        store_name,
                        lng,
                        lat,
                        address,
                        phone,
                        country,
                        state,
                        city,
                        postal_code,
                        email,
                        opening_hours,
                        store_manager,
                        store_manager_email
                        )
            values (
                        '{item['store_id']}',
                        '{item['store_name']}',
                        '{item['lng']}',
                        '{item['lat']}',
                        '{item['address']}',
                        '{item['phone']}',
                        '{item['country']}',
                        '{item['state']}',
                        '{item['city']}',
                        '{item['postal_code']}',
                        '{item['email']}',
                        '{item['opening_hours']}',
                        '{item['store_manager']}',
                        '{item['store_manager_email']}'
            )
        '''
        print(qr)
        try:
            self.cur.execute(qr)
            self.con.commit()
        except Exception as e:print(e)

    def update_store_links_status(self, store_id):
        qr = f'''
            update {self.store_links_table} set status = 1 where store_id = '{store_id}'
        '''
        self.cur.execute(qr)
        self.con.commit()