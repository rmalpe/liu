import asyncio
import os
import queue
import time
import traceback
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from pytz import timezone


from liualgotrader.common import config
from liualgotrader.common.tlog import tlog
from liualgotrader.common.types import Order, QueueMapper, Trade
from liualgotrader.trading.base import Trader
from smartapi import SmartConnect
from twelvedata import TDClient

nyc = timezone("Asia/kolkata")





def get_orders(ange_all_orders,order_id):
    for i in ange_all_orders:
        if i['orderid']== order_id:
            return i
        else:
            return "No transction found for {}".format(order_id)

class AngelTrader(Trader):
    def __init__(self, qm: QueueMapper = None):
        angel_api_key:str= config.angel_api_key
        # angel_api_secret: str = config.angel_secret_key

        self.angel_rest_client = SmartConnect("av8IlZGP")
        # self.angel_generate_session =self.angel_rest_client.generateSession("R618348","Rushi@501145")


        if qm:
            td = TDClient(apikey="7dd3c00a1d3541a3829d2c95cf16e743")
            self.tweledata_ws_client=td.websocket()
            
            if not self.tweledata_ws_client:
                raise AssertionError(
                    "Failed to authenticate Angel web_socket client"
                )
        

    async def _is_personal_order_completed(
        self, order_id: str
    ) -> Tuple[Order.EventType, float, float, float]:
        angel_all_orders = self.angel_rest_client.orderBook()['data']
        angel_order= get_orders(angel_all_orders,order_id)

        event = (
            Order.EventType.canceled
            if angel_order['status'] in ["canceled", "expired", "replaced","rejected"]
            else Order.EventType.pending
            if angel_order['status'] in ["pending_cancel", "pending_replace"]
            else Order.EventType.fill
            if angel_order['status'] == "filled"
            else Order.EventType.partial_fill
            if angel_order['status'] == "partially_filled"
            else Order.EventType.other
        )
        return (
            event,
            float(angel_order.filled_avg_price or 0.0),
            float(angel_order.filled_qty or 0.0),
            0.0,
        )

    async def is_fractionable(self, symbol: str) -> bool:
        return True

    def get_market_schedule(
        self,
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        return datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=nyc
        ), datetime.now().replace(
            hour=23, minute=59, second=59, microsecond=0, tzinfo=nyc
        )

    def get_trading_days(
        self, start_date: date, end_date: date = date.today()
    ) -> pd.DataFrame:
        return pd.DataFrame(
            index=pd.date_range(start=start_date, end=end_date)
        )


    # def get_position(self, symbol: str) -> float:
    #     # pos = self.angel_rest_client.position(symbol)
    #     # print("possssssssssssssssssss",pos)

    #     return  float(25.0)

    def to_order(self, angel_order: str) -> Order:
        event = (
            Order.EventType.canceled
            if angel_order['status'] in ["canceled", "expired", "replaced","rejected"]
            else Order.EventType.pending
            if angel_order['status'] in ["pending_cancel", "pending_replace"]
            else Order.EventType.fill
            if angel_order['status'] == "filled"
            else Order.EventType.partial_fill
            if angel_order['status'] == "partially_filled"
            else Order.EventType.other
        )
    
        return Order(
            order_id=angel_order['orderid'],
            symbol=angel_order['tradingsymbol'].lower(),
            event=event,
            price=float(angel_order['price'] or 0.0),
            side=Order.FillSide[angel_order['transactiontype']],
            filled_qty=float(angel_order['quantity']),
            remaining_amount=float(angel_order['unfilledshares'])
            - float(angel_order['quantity']),
            submitted_at=angel_order['updatetime'],
            avg_execution_price=angel_order['averageprice'],
            trade_fees=0.0,
        )
    
    async def get_order(self, order_id: str) -> Order:
        angel_all_orders = self.angel_rest_client.orderBook()['data']
        angel_order= get_orders(angel_all_orders,order_id)
        return angel_order

    def is_market_open_today(self) -> bool:
        return self.market_open is not None

    def get_time_market_close(self) -> Optional[timedelta]:
            return datetime.now().replace(
                hour=23, minute=59, second=59, microsecond=0, tzinfo=nyc
            ) - datetime.now().replace(tzinfo=nyc)

    async def reconnect(self):
        angel_api_key:str= config.angel_api_key
        angel_api_secret: str = config.angel_secret_key

        self.angel_rest_client = SmartConnect(angel_api_key)
        self.angel_generate_session =self.angel_rest_client.generateSession("R618348","Rushi@501145")

    async def run(self) -> asyncio.Task:
        if not self.running_task:
            tlog("starting Angel listener")
           
            self.running_task = asyncio.create_task(
                self.angel_ws_client._run_forever()
            )
            return self.running_task


  


    async def _cancel_personal_order(self, order_id: str) -> bool:
        self.angel_rest_client.cancelOrder(order_id,variety="NORMAL")
        return True

    async def get_tradeable_symbols(self) -> List[str]:
        if not self.angel_rest_client:
            raise AssertionError("Must call w/ authenticated Angel client")

        return [
            str(self.angel_generate_session['status']).split()
        ]

    async def get_shortable_symbols(self) -> List[str]:
        return []

    async def is_shortable(self, symbol) -> bool:
        return False



    async def submit_order(
        self,
        symbol: str,
        qty: float,
        side: str,
        order_type: str,
        limit_price: str = None,
        stop_price: str = None,
        client_order_id: str = None,
        extended_hours: bool = None,
        order_class: str = None,
        take_profit: dict = None,
        stop_loss: dict = None,
        trail_price: str = None,
        trail_percent: str = None,
        on_behalf_of: str = None,
         time_in_force: str = None,
    ) -> Order:
        symbol = symbol.lower()
        
        payload = {
            "variety": "NORMAL",
            "tradingsymbol": symbol,
            "symboltoken": "3045",
            "transactiontype": "BUY",
            "exchange": "NSE",
            "ordertype": "LIMIT",
            "producttype": "INTRADAY",
            "duration": "DAY",
            "price": limit_price,
            "squareoff": "0",
            "stoploss": "0",
            "quantity": qty
            }
        print()
        print()
        print()
        print("Payload",payload)
        print()
        print()
        print()
        self.angel_generate_session =self.angel_rest_client.generateSession("R618348","Rushi@501145")
        self.angel_rest_client.placeOrder(payload)


