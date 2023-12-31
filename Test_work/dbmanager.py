import pandas as pd
import sqlite3


class DatabaseHandler:
    def __init__(self, db_name):
        """
        Инициализация объекта для работы с базой данных.

        Args:
            db_name (str): Имя файла базы данных SQLite.
        """
        self.db_name = db_name
        self.conn = None

    def create_tables(self):
        """
        Создает таблицы в базе данных, если они не существуют.
        """
        self.conn = sqlite3.connect(self.db_name)
        cursor = self.conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS GOODS (
                ID_TOVAR INTEGER PRIMARY KEY,
                NAME_TOVAR TEXT,
                BARCOD TEXT,
                ID_COUNTRY INTEGER,
                ID_ISG INTEGER
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS COUNTRY (
                ID_COUNTRY INTEGER PRIMARY KEY,
                NAME_COUNTRY TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ISG (
                ID_ISG INTEGER PRIMARY KEY,
                NAME_ISG TEXT
            )
        ''')

        self.conn.commit()

    def import_data_from_xlsx(self, xlsx_file):
        """
        Импортирует данные из файла XLSX в базу данных и заносит уникальные страны и производителей.

        Args:
            xlsx_file (str): Путь к файлу XLSX с данными.

        Raises:
            Exception: В случае ошибки при импорте данных.
        """
        try:
            df = pd.read_excel(xlsx_file)

            df.rename(columns={
                'ID_TOVAR': 'ID_TOVAR',
                'TOVAR': 'NAME_TOVAR',
                'BARCOD': 'BARCOD',
                'ID_ISG': 'ID_ISG',
                'ISG': 'NAME_ISG',
                'COUNTRY': 'NAME_COUNTRY'
            }, inplace=True)

            df.to_sql('GOODS', self.conn, if_exists='replace', index=False)

            unique_countries = df['NAME_COUNTRY'].unique()
            cursor = self.conn.cursor()
            for country in unique_countries:
                cursor.execute('INSERT INTO COUNTRY (NAME_COUNTRY) VALUES (?)', (country,))
            self.conn.commit()

            unique_isgs = df['NAME_ISG'].unique()
            for isg in unique_isgs:
                cursor.execute('INSERT INTO ISG (NAME_ISG) VALUES (?)', (isg,))
            self.conn.commit()
        except Exception as e:
            print(f"Произошла ошибка при импорте данных из файла XLSX: {e}")

    def count_goods_per_country(self):
        """
        Подсчитывает количество товаров по странам и возвращает результат.

        Returns:
            list of tuple: Список кортежей, где каждый кортеж содержит имя страны и количество товаров.
        """
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT GOODS.NAME_COUNTRY, COUNT(GOODS.ID_TOVAR)
            FROM GOODS
            GROUP BY GOODS.NAME_COUNTRY
        ''')
        result = cursor.fetchall()
        return result

    def close(self):
        """
        Закрывает соединение с базой данных.
        """
        self.conn.close()
