from Test_work.dbmanager import DatabaseHandler

if __name__ == "__main__":
    db_handler = DatabaseHandler('test_work_db.sqlite')

    db_handler.create_tables()

    db_handler.import_data_from_xlsx('data.xlsx')

    goods_per_country = db_handler.count_goods_per_country()

    with open('data.tsv', 'w') as file:
        for row in goods_per_country:
            file.write(f"{row[0]} - {row[1]}\n")

    db_handler.close()
