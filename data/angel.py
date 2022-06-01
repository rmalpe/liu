import asyncio
import concurrent.futures
from locale import currency
import queue
import time
import traceback
from datetime import date, datetime, timedelta
from random import randint
from typing import Callable, Dict, List, Optional, Tuple
from threading import Thread
import numpy as np
import pandas as pd
import pandas_market_calendars
import pytz
import requests
from smartapi import SmartConnect
# from alpaca_trade_api.rest import REST, URL, APIError, TimeFrame
# from alpaca_trade_api.stream import Stream
from dateutil.parser import parse as date_parser
from twelvedata import TDClient
from liualgotrader.common import config
from liualgotrader.common.list_utils import chunks
from liualgotrader.common.tlog import tlog, tlog_exception
from liualgotrader.common.types import QueueMapper, TimeScale, WSEventType
from liualgotrader.data.data_base import DataAPI
from liualgotrader.data.streaming_base import StreamingAPI

            
NY = "Asia/kolkata"
nytz = pytz.timezone(NY)

class AngelData(DataAPI):
    def __init__(self):

        self.angel_rest_client = SmartConnect(
            config.angel_api_key
        )
        # self.angel_generate_session =self.angel_rest_client.generateSession("R618348","Rushi@501145")
        # print(self.angel_generate_session,"***************")
        if not self.angel_rest_client:
            raise AssertionError(
                "Failed to authenticate Angel RESTful client"
            )
        # for requesting market snapshots by chunk of symbols
        self.symbol_chunk_size = 1000
        self.datetime_cache: Dict[datetime, datetime] = {}

    def get_symbols(self) -> List[str]:
        if not self.angel_rest_client:
            raise AssertionError("Must call w/ authenticated Angel client")

        return [
            str(self.angel_generate_session['status']).split()
        ]

    def get_market_snapshot(self, filter_func) -> List[Dict]:
        raise NotImplementedError

    def get_symbol_data(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        scale: TimeScale = TimeScale.hour,
    ) -> pd.DataFrame:
        interval=str(1)+"h"
        url="https://api.twelvedata.com/time_series?symbol={}&interval={}&apikey=7dd3c00a1d3541a3829d2c95cf16e743".format(symbol,interval)
        res=requests.get(url)
        data= pd.DataFrame.from_dict(res.json()['values'])
        data.rename(
            columns={
                "o": "open",
                "c": "close",
                "h": "high",
                "l": "low",
                "v": "volume",
            },
            inplace=True,
        )

        if data.empty:
                raise ValueError(
                f"[ERROR] {symbol} has no data for"
            )
        return data


    def get_symbols_data(
        self,
        symbols: List[str],
        start: date,
        end: date = date.today(),
        scale: TimeScale = TimeScale.minute,
    ) -> Dict[str, pd.DataFrame]:
        raise NotImplementedError("get_symbols_data")


    def get_last_trading(self, symbol: str) -> datetime:
            raise NotImplementedError("get_last_trading")

    def get_trading_day(
        self, symbol: str, now: datetime, offset: int
    ) -> datetime:
        raise NotImplementedError("get_trading_day")

    def get_market_snapshot(self, filter_func) -> List[Dict]:
        raise NotImplementedError

    def trading_days_slice(self, symbol: str, slice) -> slice:
        raise NotImplementedError("trading_days_slice")

    def num_trading_minutes(self, symbol: str, start: date, end: date) -> int:
        raise NotImplementedError("num_trading_minutes")

    def num_trading_days(self, symbol: str, start: date, end: date) -> int:
        raise NotImplementedError("num_trading_days")

    def get_max_data_points_per_load(self) -> int:
        raise NotImplementedError("get_max_data_points_per_load")

    def get_last_trading(self, symbol: str) -> datetime:
        return datetime.now(nytz)

    def get_trading_day(
        self, symbol: str, now: datetime, offset: int
    ) -> datetime:
        return (
            nytz.localize(datetime.combine(now, datetime.min.time()))
            if isinstance(now, date)
            else now
        ) + timedelta(days=offset)





class AngelStream(StreamingAPI):

    def __init__(self, queues: QueueMapper):
        self.running_task: Optional[Thread] = None
        self.ws = None
        super().__init__(queues)



    async def run(self):
        td = TDClient(apikey="7dd3c00a1d3541a3829d2c95cf16e743")
        self.ws =td.websocket(event=AngelStream.trades_handler)
        self.running_task = Thread(target=self.ws.keep_alive)
        self.running_task.start()
        return self.running_task

        

    @classmethod
    async def bar_handler(cls, msg):
        try:
            event = {
                "symbol": msg.symbol,
                "open": msg.open,
                "close": msg.close,
                "high": msg.high,
                "low": msg.low,
                "timestamp": pd.to_datetime(
                    msg.timestamp, utc=True
                ).astimezone(nytz),
                "volume": msg.volume,
                "count": msg.trade_count,
                "vwap": None,
                "average": msg.vwap,
                "totalvolume": None,
                "EV": "AM",
            }
            cls.get_instance().queues[msg.symbol].put(event, timeout=1)
        except queue.Full as f:
            tlog(
                f"[EXCEPTION] process_message(): queue for {event['sym']} is FULL:{f}"
            )
            raise
        except Exception as e:
            tlog(
                f"[EXCEPTION] process_message(): exception of type {type(e).__name__} with args {e.args}"
            )
            if config.debug_enabled:
                traceback.print_exc()

    @classmethod
    def trades_handler(cls, msg):
            try:
                        symbol= msg['symbol']
                        price=msg['price']
                        # exchange=msg['exchange']
                        currency=msg['currency']
          
                        event = {
                            "symbol":symbol,
                            "price": price,
                            "open": price,
                            "close": price,
                            "high": price,
                            "low": price,
                            # "exchange": exchange,
                            "conditions": currency,
                            "average": None,
                            "count": 1,
                            "vwap": None,
                            "volume": "1000",
                            "timestamp":datetime.fromtimestamp(msg['timestamp']),
                            "EV": "T",
                        }

                        cls.get_instance().queues[msg['symbol']].put(event, block=False)
                        print()
                        print("event is ",event)
                        print()
                    
            except queue.Full as f:
                tlog(
                    f"[EXCEPTION] process_message(): queue for {event['sym']} is FULL:{f}"
                )
                raise
            except Exception as e:
                tlog(
                    f"[EXCEPTION] process_message(): exception of type {type(e).__name__} with args {e.args}"
                )
                if config.debug_enabled:
                    traceback.print_exc()

    @classmethod
    async def quotes_handler(cls, msg):
        print(f"quotes_handler:{msg}")

    async def subscribe(
        self, symbols: List[str], events: List[WSEventType]
    ) -> bool:
        tlog(f"Starting subscription for {len(symbols)} symbols")
        upper_symbols = [symbol.upper() for symbol in symbols]
        for syms in chunks(upper_symbols, 1000):
            tlog(f"\tsubscribe {len(syms)}/{len(upper_symbols)}")
            for event in events:
                if event == WSEventType.TRADE:
                    td = TDClient(apikey="7dd3c00a1d3541a3829d2c95cf16e743")
                    self.tweledata_ws_client=td.websocket(on_event=AngelStream.trades_handler)
                    self.tweledata_ws_client.subscribe(syms)
                    self.tweledata_ws_client.connect()
                    
                elif event == WSEventType.QUOTE:
                    pass

            await asyncio.sleep(30)

        tlog(f"Completed subscription for {len(symbols)} symbols")
        return True