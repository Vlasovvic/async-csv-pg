import numpy as np
import pandas
import asyncio
import asyncpg
import json

class ClassName:
    Series_reference: str
    Period: float
    Data_value: float
    Suppressed: bool
    STATUS: chr
    UNITS: str
    Magnitude: int
    Subject: str
    Groups: str
    Series_title_1: str
    Series_title_2: str
    Series_title_3: str
    Series_title_4: str
    Series_title_5: str


class Database:
    def __init__(self):
        self.actual_table = f"actual_finance"
        self.period_table = f"period_table"
        self.history_table = f"history_table"
        self.__KNOWN_SQL_COLUMNS_FULL = f"Series_reference, Period, Data_value, Suppressed, STATUS, UNITS, Magnitude,  Subject, Groups, Series_title_1, Series_title_2,Series_title_3, Series_title_4, Series_title_5 "
        self.__KNOWN_SQL_COLUMNS_INSERTABLE = f"1,2,3,4,5,6,7,8"

    def __SQL_HELPER_COLUMNS_FULL(self, row) -> ClassName:
        result = ClassName()
        if row[0] is not None:
            result.Series_reference = int(row[0])
        if row[1] is not None:
            result.Period = int(row[1])
        if row[2] is not None:
            result.Data_value = float(row[2])
        if row[3] is not None:
            result.Suppressed = float(row[3])
        if row[4] is not None:
            result.STATUS = chr(row[4])
        if row[5] is not None:
            result.UNITS = str(row[5])
        if row[6] is not None:
            result.Magnitude = int(row[6])
        if row[7] is not None:
            result.Subject = str(row[7])
        if row[8] is not None:
            result.Groups = float(row[8])
        if row[9] is not None:
            result.Series_title_1 = float(row[9])
        if row[10] is not None:
            result.Series_title_2 = float(row[10])
        if row[11] is not None:
            result.Series_title_3 = float(row[11])
        if row[12] is not None:
            result.Series_title_4 = float(row[12])
        if row[13] is not None:
            result.Series_title_5 = float(row[13])
        return result

    async def query_cursor_execute(self, connection, query):

        result = []

        try:
            cursor = connection.cursor()
            try:
                cursor.execute(query)
                # result = cursor.fetchall()
                if cursor.pgresult_ptr is not None:
                    result = cursor.fetchall()
            except Exception as e:
                connection.commit()
                raise e
            finally:
                connection.commit()
                cursor.close()
        except Exception as e:
            raise e

        return result

    async def crate(self, data, connection, table):
        query = f'''
            INSERT INTO "{table}"({self.__KNOWN_SQL_COLUMNS_FULL})
            VALUES (
            '{data.Series_reference}', {data.Period}, {data.Data_value}, '{data.Suppressed}', '{data.STATUS}', '{data.UNITS}', {data.Magnitude},  '{data.Subject}',
             '{data.Groups}', '{data.Series_title_1}', '{data.Series_title_2}', '{data.Series_title_3}', '{data.Series_title_4}', '{data.Series_title_5}' 
            )
            RETURNING {self.__KNOWN_SQL_COLUMNS_FULL}
            '''

        async with connection.acquire() as con:
            await con.execute(query)

            # return result

    async def read(self, table_name, table, connection):
        query = f'''
            SELECT * FROM "{table_name}" 
            WHERE Series_reference = '{table.Series_reference}' and Period = {table.Period};
            '''
        async with connection.acquire() as con:

            result = await con.fetch(query)

            print("read result = ", result)

            return result

    async def update(self, new_data, connection):
        async with connection.transaction():
            async for result in connection.cursor(f'''
            UPDATE ""
            SET 
              dt = '{new_data.dt}',
              temp = {new_data.temp}, min_temp = {new_data.min_temp}, max_temp = {new_data.max_temp},
              ts = to_timestamp({new_data.ts}), updated = to_timestamp({new_data.updated}),
              fraw = '{new_data.fraw}'
            WHERE dt = '{new_data.dt}'
            RETURNING {self.__KNOWN_SQL_COLUMNS_FULL}
            '''):
                """ response = await self.query_cursor_execute(result)
 
                 for row in response:
                     result = self.__SQL_HELPER_COLUMNS_FULL(row)"""

            return result

    async def delete(self, table_name, table, connection):
        async with connection.transaction():
            async for result in connection.cursor(f'''
            DELETE FROM {table_name}
            WHERE Series_reference = {table.Series_reference} AND Period = {table.Period}
            RETURNING {self.__KNOWN_SQL_COLUMNS_FULL}
            '''):
                """response = await self.query_cursor_execute(result,connection)

                for row in response:
                    result = self.__SQL_HELPER_COLUMNS_FULL(row)"""

            return result


async def actual_data(file, connection):
    # сортировка актуальных данных
    table = "actual_finance"
    output = []
    sort = file.sort_values(by=['Period'], ascending=False)
    # print(sort.dtypes)
    # print(sort['Period'].max())
    df_filter = sort['Period'].isin([sort['Period'].max()])

    actual_table = sort[df_filter]

    # запись в бд
    for elem in file.itertuples(index=False):
        await Database().crate(elem, connection, table)


""" for elem in file.itertuples(index=False):
        temp = await Database().read(table, elem, connection)
        output.append(temp)
    print(output)"""


async def period_data(file, start_period, end_period, connection):
    table = "period_finance"
    output = []
    period_table = file.query(f'{start_period}<=Period<={end_period}')
    # запись в бд
    for elem in period_table.itertuples(index=False):
        await Database().crate(elem, connection, table)
    for elem in period_table.itertuples(index=False):
        temp = await Database().read(table, elem, connection)
        output.append(temp)
    print(output)


async def history_data(file, connection):
    table = "history_data"
    # DB

    pass


async def job():
    with open('config.json') as file:
        config = json.load(file)
    Known_columns = ['Series_reference', 'Period', 'Data_value', 'Suppressed', 'STATUS', 'UNITS', 'Magnitude',
                     'Subject', 'Groups',
                     'Series_title_1', 'Series_title_2', 'Series_title_3', 'Series_title_4', 'Series_title_5']
    file = pandas.read_csv('data/business-financial-data-june-2022-quarter-csv.csv', na_filter=False)
    file = file.set_axis(Known_columns, axis=1, inplace=False)
    file = file.reset_index(drop=True)
    file['Suppressed'] = 'False'
    file.loc[file.Data_value == "", ('Data_value')] = 'null'
    file = file.round(decimals=2)
    print(file)
    start_period = input(f"start_period= ")
    end_period = input(f"end_period= ")
    connection = await asyncpg.create_pool(user=config['user'], password=config['password'], host=config['host'], port=config['port'],
                                           database='findb', min_size=50, max_size=100, command_timeout=120)
    task1 = asyncio.create_task(actual_data(file, connection))
    task2 = asyncio.create_task(period_data(file, start_period, end_period, connection))
    task3 = asyncio.create_task(actual_data(file, connection))

    await task1
    #await task2
    #await task3


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(job())
