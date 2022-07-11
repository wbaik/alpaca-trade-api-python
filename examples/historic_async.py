from enum import Enum
import time
import alpaca_trade_api as tradeapi
import asyncio
import os
import pandas as pd
import sys
from alpaca_trade_api.rest import TimeFrame, URL
from alpaca_trade_api.rest_async import gather_with_concurrency, AsyncRest

NY = 'America/New_York'


class DataType(str, Enum):
    Bars = "Bars"
    Trades = "Trades"
    Quotes = "Quotes"


def get_data_method(data_type: DataType):
    if data_type == DataType.Bars:
        return rest.get_bars_async
    elif data_type == DataType.Trades:
        return rest.get_trades_async
    elif data_type == DataType.Quotes:
        return rest.get_quotes_async
    else:
        raise Exception(f"Unsupoported data type: {data_type}")


async def get_historic_data_base(symbols, data_type: DataType, start, end,
                                 timeframe: TimeFrame = None):
    """
    base function to use with all
    :param symbols:
    :param start:
    :param end:
    :param timeframe:
    :return:
    """
    major = sys.version_info.major
    minor = sys.version_info.minor
    if major < 3 or minor < 6:
        raise Exception('asyncio is not support in your python version')
    msg = f"Getting {data_type} data for {len(symbols)} symbols"
    msg += f", timeframe: {timeframe}" if timeframe else ""
    msg += f" between dates: start={start}, end={end}"
    results = []
    tasks = []
    for symbol in symbols:
        args = [symbol, start, end, timeframe.value] if timeframe else [symbol, start, end]
        tasks.append(asyncio.create_task(get_data_method(data_type)(*args)))
    done, pending = await asyncio.wait(tasks, timeout=600.0)
    results.extend([item.result() for item in done])

    bad_requests = 0
    for response in results:
        if isinstance(response, Exception):
            print(f"Got an error: {response}")

    print(f"Total of {len(results)} {data_type}, and {bad_requests} "
          f"empty responses.")
    print("Total count of done: {} of the total tickers count: {}".format(len(done), len(symbols)))
    return results


async def get_historic_bars(symbols, start, end, timeframe: TimeFrame):
    return await get_historic_data_base(symbols, DataType.Bars, start, end, timeframe)

async def get_historic_trades(symbols, start, end, timeframe: TimeFrame):
    return await get_historic_data_base(symbols, DataType.Trades, start, end)


async def get_historic_quotes(symbols, start, end, timeframe: TimeFrame):
    return await get_historic_data_base(symbols, DataType.Quotes, start, end)


async def main(symbols):
    # start = pd.Timestamp('2022-02-10', tz=NY).date().isoformat()
    # end = pd.Timestamp('2022-02-10', tz=NY).date().isoformat()
    # start = pd.Timestamp('2021-02-10 09:30:00', tz='UTC').date().isoformat()
    # end = pd.Timestamp('2021-02-10 09:45:00', tz='UTC').date().isoformat()
    start, end = '2021-02-10T16:30:00Z', '2021-02-10T16:45:00Z'
    timeframe: TimeFrame = TimeFrame.Minute
    # a = await get_historic_bars(symbols, start, end, timeframe)
    # b = await get_historic_trades(symbols, start, end, timeframe)
    c = await get_historic_quotes(symbols, start, end, timeframe)
    # return a, b, c
    return c

if __name__ == '__main__':
    api_key_id = os.environ.get('APCA_API_KEY_ID')
    api_secret = os.environ.get('APCA_API_SECRET_KEY')
    # base_url = "https://paper-api.alpaca.markets"
    # feed = "sip"  # change to "sip" if you have a paid account

    rest = AsyncRest(key_id=api_key_id,
                     secret_key=api_secret)

    api = tradeapi.REST(key_id=api_key_id,
                        secret_key=api_secret,)
                        # base_url=URL(base_url))

    start_time = time.time()
    # symbols = [el.symbol for el in api.list_assets(status='active')]
    symbols = ['AAPL', 'AMZN', 'GS', 'JPM', 'GOOGL', 'ABNB',
               'BARK', 'APRN', 'CHWY', 'CAG', 'LLY', 'FND', 'EDU',
               'PYPL', 'PEBO', "RBLX", 'SHAK', 'SHOP', 'TDOC', 'VRTX', 'VICI']

    # a, b, c = asyncio.run(main(symbols))
    c = asyncio.run(main(symbols))
    print(f"took {time.time() - start_time} sec")

    # print("1.Result of historic-bar is as follows")
    # for ticker, dataframe in a:
    #     if not dataframe.empty:
    #         print("Ticker: {}".format(ticker))
    #         print(dataframe)
    #         print(dataframe.describe())

    # print("2.Result of historic-trade is as follows")
    # for ticker, dataframe in b:
    #     if not dataframe.empty:
    #         print("Ticker: {}".format(ticker))
    #         print(dataframe)
    #         print(dataframe.describe())

    print("3.Result of historic-quote is as follows")
    for ticker, dataframe in c:
        if not dataframe.empty:
            print("Ticker: {}".format(ticker))
            print(dataframe)
            print(dataframe.describe())
