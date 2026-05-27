"""
PyEventBT
Documentation: https://pyeventbt.com
GitHub: https://github.com/marticastany/pyeventbt

Author: Marti Castany
Copyright (c) 2025 Marti Castany
Licensed under the Apache License, Version 2.0
"""

from ..core.interfaces.execution_engine_interface import IExecutionEngine
from ..core.configurations.execution_engine_configurations import MT5LiveExecutionConfig
from pyeventbt.data_provider.core.interfaces.data_provider_interface import IDataProvider
from pyeventbt.broker.mt5_broker.core.entities.order_send_result import OrderSendResult
from pyeventbt.broker.mt5_broker.core.entities.trade_deal import TradeDeal
from pyeventbt.events.events import BarEvent, FillEvent, OrderEvent
from pyeventbt.utils.utils import Utils, check_platform_compatibility
from typing import Callable
from datetime import datetime
from queue import Queue
from decimal import Decimal
from pyeventbt.portfolio.core.entities.open_position import OpenPosition
from pyeventbt.portfolio.core.entities.pending_order import PendingOrder
import time
import logging

try:
    if check_platform_compatibility(raise_exception=False):
        import MetaTrader5 as mt5
    else:
        mt5 = None
except ImportError:
    mt5 = None

logger = logging.getLogger("pyeventbt")


class Mt5LiveExecutionEngineConnector(IExecutionEngine):

    def __init__(self, configs: MT5LiveExecutionConfig, events_queue: Queue, data_provider: IDataProvider):
        """
        Args:
            events_queue (Queue): The event queue.
        """

        self.events_queue = events_queue
        self.DATA_PROVIDER = data_provider
        self.pending_orders: list[OrderSendResult] = [] #list of OrderSendResult objects from the pending orders (all will have same MagicNumber)
        self.magic_number = configs.magic_number
        #self.account_currency = self.get_account_currency()

    def _check_common_trade_values(self, volume: float = 0.0, price: float = 0.0, stop_loss: float = 0.0, take_profit: float = 0.0,
                                    magic: int  = 0, deviation: int = 0, comment: str = '') -> bool:
            """
            Check if the given trade values are valid.

            Args:
            volume (float): The volume of the trade.
            price (float): The price of the trade.
            stop_loss (float): The stop loss value of the trade.
            take_profit (float): The take profit value of the trade.
            magic (int): The magic number of the trade.
            deviation (int): The deviation value of the trade.
            comment (str): The comment for the trade.

            Returns:
            bool: True if all values are valid, False otherwise.
            """
            # Check if volume is valid
            if volume <= 0:
                logger.error(f"Invalid volume: {volume}")
                return False
            
            # Check if price is valid
            if price < 0:
                logger.error(f"Invalid price: {price}")
                return False
            
            # Check if stop_loss is valid
            if stop_loss < 0:
                logger.error(f"Invalid stop loss: {stop_loss}")
                return False
            
            # Check if take_profit is valid
            if take_profit < 0:
                logger.error(f"Invalid take profit: {take_profit}")
                return False
            
            # Check if magic number is valid
            if magic < 0:
                logger.error(f"Invalid magic number: {magic}")
                return False
            
            # Check if deviation is valid
            if deviation < 0:
                logger.error(f"Invalid deviation: {deviation}")
                return False
            
            # Check if comment is valid
            if len(comment) > 31:
                logger.error(f"Invalid comment: {comment}")
                return False
            
            return True

    def _check_succesful_order_execution(self, result: OrderSendResult) -> bool:
        """
        Check if the order was executed successfully.

        Args:
        result (OrderSendResult): The result of the order execution.

        Returns:
        bool: True if the order was executed successfully, False otherwise.
        """
        if result is None:
            print(f"{Utils.dateprint()} - Order failed. No result returned.")
            return False
        elif result.retcode == mt5.TRADE_RETCODE_DONE or result.retcode == mt5.TRADE_RETCODE_NO_CHANGES:
            return True
        else:
            #print(f"{Utils.dateprint()} - WARNING: Order for {result.request.symbol} failed with retcode: {result.retcode}. Last error: {mt5.last_error()}")
            return False

    def _generate_and_put_fill_event(self, trade_deal: TradeDeal, events_queue: Queue) -> None:
        """
        Generates a fill event for an executed DEAL and puts it in the event queue.

        Args:
        order_result (OrderSendResult): The result of the order execution.
        events_queue (Queue): The queue where the fill event will be put.

        Returns:
        None
        """

        # Generate the fill event
        fill_event = FillEvent(deal="IN" if trade_deal.entry == 0 else "OUT",
                                symbol=trade_deal.symbol,
                                time_generated=trade_deal.time_msc,
                                position_id=trade_deal.position_id,
                                strategy_id=str(trade_deal.magic),
                                exchange='MT5',
                                volume=trade_deal.volume,
                                price=trade_deal.price,
                                signal_type='BUY' if trade_deal.type == 0 else 'SELL',  # TODO: it now does not take into account a deal type of type credit, balance, etc
                                commission=abs(trade_deal.commission),
                                swap=trade_deal.swap,
                                fee=trade_deal.fee,
                                gross_profit=trade_deal.profit,
                                ccy=self._get_account_currency())
        
        # Put the fill event in the event queue
        events_queue.put(fill_event)

    def _check_if_pending_orders_filled(self, bar_event: BarEvent) -> None:
        """
        Checks if any pending order have been filled in the symbol of the BarEvent and generates a fill event, for every order, if they have.
        """
        # Check if there are any pending orders
        if self.pending_orders == []:
            return
        
        # Get the positions for the symbol of the BarEvent and check if there are any positions
        positions = mt5.positions_get(symbol=bar_event.symbol)  # Returns a tuple of TradePosition objects
        if positions == ():
            return

        # Check if the pending orders have been executed. The pending order 'order' code becomes the position 'ticket' code.
        # We use a slice to create a COPY of the list for iteration, as we will be removing elements from the list while iterating
        for pending_order in self.pending_orders[:]:
            # Check if the order is for the symbol of the BarEvent
            if pending_order.request.symbol != bar_event.symbol:
                continue

            # Check if the order code is in the positions. If it is, it means the order has been executed
            for position in positions:
                if pending_order.order == position.identifier:
                    logger.info(f"Pending order {pending_order.order} in {bar_event.symbol} has been filled")
                    
                    # Now we need to get the deal(s) information about the executed order (an order can be executed in multiple deals)
                    deals = mt5.history_deals_get(position=position.identifier)

                    # If the deals tuple is empty, we'll need to wait for the server to fill
                    if not deals:
                        try_count = 0
                        while try_count < 20:
                            time.sleep(0.05)
                            deals = mt5.history_deals_get(position=position.identifier)
                            if not deals:
                                try_count += 1
                                continue
                            else:
                                break
                    
                    # If still after 20 tries (1 second) we don't have the deals, we'll have to skip this order and wait for the server to fill
                    if not deals:
                        continue
                    
                    for deal in deals:
                        # Generate and put the fill event
                        self._generate_and_put_fill_event(trade_deal=deal, events_queue=self.events_queue)

                    # Remove the order from the original list of pending orders
                    self.pending_orders.remove(pending_order)

                    # Break the loop as we have found the order code in the positions
                    break

    def _get_desired_trade_method(self, order_event: OrderEvent) -> Callable:
        
        ## We will return the adecuate method to get the desired trade depending on the buffer_data
        if order_event.buffer_data is not None:
            return self._get_buffered_desired_trade
        else:
            return self._get_desired_trade_cfd

    def get_strategy_open_volume_by_symbol(self, symbol:str) -> Decimal:
        
        #Getting strategy positions in this symbol
        positions = self._get_strategy_positions(symbol=symbol)

        total_volume = Decimal(0)
        
        for position in positions:
            pos_vol = Decimal(str(position.volume))
            
            if position.type == "BUY":
                total_volume += pos_vol
            else:
                total_volume -= pos_vol
            
        return total_volume

    def _process_order_event(self, order_event: OrderEvent) -> None:

        if order_event.order_type == "MARKET":
            self._send_market_order(order_event)
        elif order_event.order_type == "CONT":
            # For continuous system, the target position is in the order_event.volume
            self.execute_desired_continuous_trade(order_event)
            #pass
        else:
            self._send_pending_order(order_event)

    def _update_values_and_check_executions_and_fills(self, bar_event: BarEvent) -> None:
        pass

    def _send_market_order(self, order_event: OrderEvent) -> OrderSendResult:
        """
        Executes a Market Order and returns an OrderSendResult object.
        Generates a FillEvent on success.
        """
        symbol = order_event.symbol
        magic = int(order_event.strategy_id)   #For mt5, strategy id must be made only with numbers
        comment = order_event.strategy_id + "-MKT"

        # Check if symbol is in Market Watch
        if not mt5.symbol_info(symbol).visible:
            logger.error(f"Symbol {symbol} is not in Market Watch. Please add it to Market Watch first.")
            return 0
        
        # Check if order type (signal_type in our Domain, as order type is MKT, LIMIT, etc) is valid. Here we are mapping our domain BUY SELL signals to the MT5 order types.
        if order_event.signal_type == "BUY":
            signal_type_int = mt5.ORDER_TYPE_BUY
        elif order_event.signal_type == "SELL":
            signal_type_int = mt5.ORDER_TYPE_SELL
        else:
            signal_type_int = -1
        if signal_type_int not in [mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_SELL]:
            logger.error(f"Invalid order type: {signal_type_int}")
            return 0
        
        # Check the trade values are valid
        if not self._check_common_trade_values(volume=order_event.volume, stop_loss=order_event.sl, take_profit=order_event.tp, magic=magic, deviation=0, comment=comment):
            return 0
        
        # Check order volume is less than the maximum allowed volume for the symbol
        if float(order_event.volume) > mt5.symbol_info(symbol).volume_max:
            logger.warning(f"Volume {order_event.volume} is greater than the maximum allowed volume for {symbol}. Adjusting to maximum allowed volume.")
            volume = mt5.symbol_info(symbol).volume_max
        else:
            volume = float(order_event.volume)
        
        # Generate the market order request
        # Creating the trade request
        market_order_request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": volume,
            'sl': float(order_event.sl),
            'tp': float(order_event.tp),
            "type": signal_type_int,
            "magic": magic,
            "comment": comment,
            'deviation': 0,
            "type_filling": mt5.ORDER_FILLING_FOK,
        }

        # Send the order request and get the result as a OrderSendResult object
        result = mt5.order_send(market_order_request)

        # Check if the order was executed successfully
        if self._check_succesful_order_execution(result):
            logger.info(f"Market Order Filled: Order #{result.order} {order_event.signal_type} {order_event.volume} {symbol} at {result.price:.5f}")

            # Get the deal(s) information about the executed order (an order can be executed in multiple deals)
            #deals = mt5.history_deals_get(ticket=result.deal)
            deals = mt5.history_deals_get(position=result.order)  # in LIVE, result gives 0 in the deal field, so we need to use the position field (waiting for fix from MetaQuotes)

            # If the deals tuple is empty, we'll need to wait for the server to fill
            if not deals:
                try_count = 0
                while try_count < 100:
                    time.sleep(0.05)
                    deals = mt5.history_deals_get(position=result.order)
                    if not deals:
                        try_count += 1
                        continue
                    else:
                        break
            
            # If still after 100 tries (5 seconds) we don't have the deals, we'll have to skip this order and wait for the server to fill
            if not deals:
                logger.warning(f"Market Order for {symbol} failed (probably executed), but NO deals returned.")
                dict_result = result._asdict()
                dict_result['request'] = result.request._asdict()
                return OrderSendResult(**dict_result)
            
            for deal in deals:
                # Generate and put the fill event
                self._generate_and_put_fill_event(trade_deal=deal, events_queue=self.events_queue)

        else:
            logger.warning(f"Market Order for {symbol} failed with retcode: {result.retcode}. Last error: {mt5.last_error()}")
        
        # Convert the result to a dictionary and add the request as a dictionary too. Then convert it back to an OrderSendResult object
        dict_result = result._asdict()
        dict_result['request'] = result.request._asdict()

        # Return the OrderSendResult object. This is to ensure the result is always returned as an OrderSendResult object as defined in the our domain
        return OrderSendResult(**dict_result)

    def _send_pending_order(self, order_event: OrderEvent) -> OrderSendResult:
        """Sends a pending order"""
        
        symbol = order_event.symbol
        magic = int(order_event.strategy_id)   #For mt5, strategy id must be made only with numbers
        comment = order_event.strategy_id + "-PDG"
        signal_type = order_event.signal_type
        order_type = order_event.order_type
        volume = order_event.volume
        stop_loss = order_event.sl
        take_profit = order_event.tp
        
        # Check if symbol is in Market Watch
        if not mt5.symbol_info(symbol).visible:
            logger.error(f"Symbol {symbol} is not in Market Watch. Please add it to Market Watch first.")
            return 0
        
        # Check if order type is valid
        if order_type == "LIMIT":
            if signal_type == "BUY":
                order_type_int = mt5.ORDER_TYPE_BUY_LIMIT
            else:
                order_type_int = mt5.ORDER_TYPE_SELL_LIMIT
        elif order_type == "STOP":
            if signal_type == "BUY":
                order_type_int = mt5.ORDER_TYPE_BUY_STOP
            else:
                order_type_int = mt5.ORDER_TYPE_SELL_STOP
        else:
            order_type_int = -1
        
        if order_type_int not in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT, mt5.ORDER_TYPE_BUY_STOP, mt5.ORDER_TYPE_SELL_STOP]:
            logger.error(f"Invalid order type: {order_type_int})")
            return 0
        
        # Check the trade values are valid
        if not self._check_common_trade_values(volume=volume, stop_loss=stop_loss, take_profit=take_profit, magic=magic, deviation=0, comment=comment):
            return 0
        
        # Check if expiration is valid
        # if expiration < datetime.now():
        #     print(f"Invalid expiration: {expiration}")
        #     return 0
        
        # Creating the trade request
        pending_request = {
            "action": mt5.TRADE_ACTION_PENDING,
            "symbol": symbol,
            "volume": float(volume),
            "type": order_type_int,
            "price": float(order_event.order_price),
            "sl": float(stop_loss),
            "tp": float(take_profit),
            "deviation": 0,
            "magic": magic,
            "comment": comment,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_FOK,
            # "expiration": expiration
        }

        # Send the order request and get the result as a OrderSendResult object
        result = mt5.order_send(pending_request)

        # Check if the order was placed successfully
        if self._check_succesful_order_execution(result):
            logger.info(f"Pending Order Sent: Order #{result.order} {order_event.signal_type} {order_event.order_type} {volume} {symbol} at {result.price:.5f}")
        else:
            if result is None:
                logger.warning(f"Pending Order for {symbol} failed. Result is None. Last error: {mt5.last_error()}")
                return None
            else:
                logger.warning(f"Pending Order for {symbol} failed with retcode: {result.retcode}. Last error: {mt5.last_error()}")


        # Convert the result to a dictionary and add the request as a dictionary too. Then convert it back to an OrderSendResult object
        dict_result = result._asdict()
        dict_result['request'] = result.request._asdict()

        # Add the pending order OrderSendResult object to the list of pending orders
        self.pending_orders.append(OrderSendResult(**dict_result))

        # Return the OrderSendResult object. This is to ensure the result is always returned as an OrderSendResult object as defined in our domain
        return OrderSendResult(**dict_result)

    def close_position(self, position_ticket: int, partial_volume:Decimal = Decimal('0.0')) -> OrderSendResult:
        """
        Closes a currently opened position by ticket. Also allows partial close if partial_volume is passed

        Args:
            position_ticket (int): The ticket number of the position to be closed.

        Returns:
            OrderSendResult: An object containing information about the result of the order execution.
        """

        # Check if position ticket is valid
        if position_ticket < 0:
            logger.error(f"Invalid order ticket: {position_ticket}")
            return False
        
        # Check if position exists
        positions = mt5.positions_get(ticket=position_ticket)
        if positions == ():
            logger.error(f"Position {position_ticket} does not exist.")
            return False
        else:
            position = positions[0]    
        
        # Defining the volume we need to close to account for a partial close
        close_volume = partial_volume if partial_volume > 0 else Decimal(str(position.volume))
        
        close_comment = f"{position.magic}-Close position" if partial_volume == 0 else f"{position.magic}-Partial Close"

        # Creating the trade request
        close_request = {
            'action': mt5.TRADE_ACTION_DEAL,
            'position': position.ticket,
            'symbol': position.symbol,
            'volume': float(close_volume),
            'type': mt5.ORDER_TYPE_BUY if position.type == 1 else mt5.ORDER_TYPE_SELL,
            'type_filling': mt5.ORDER_FILLING_FOK,
            'magic': position.magic,
            'comment': close_comment
        }

        # Send the order request and get the result as a OrderSendResult object
        result = mt5.order_send(close_request)

        # Check if the order was executed successfully
        if self._check_succesful_order_execution(result):
            # Get the deal(s) information about the executed order (an order can be executed in multiple deals)
            #deal = mt5.history_deals_get(ticket=result.deal)[0]
            # waiting for fix from metaquotes as live accounts return 0 in the deal field in order_send
            deals = mt5.history_deals_get(position=result.order)  # in LIVE, result gives 0 in the deal field, so we need to use the position field (waiting for fix from MetaQuotes)

            # If the deals tuple is empty, we'll need to wait for the server to fill
            if not deals:
                try_count = 0
                while try_count < 100:
                    time.sleep(0.05)
                    deals = mt5.history_deals_get(position=result.order)
                    if not deals:
                        try_count += 1
                        continue
                    else:
                        break
            
            # If still after 100 tries (5 seconds) we don't have the deals, we'll have to skip this order and wait for the server to fill
            if not deals:
                logger.warning(f"Closing position for {position.symbol} failed (probably closed), but NO deals returned.")
                # Convert the result to a dictionary and add the request as a dictionary too. Then convert it back to an OrderSendResult object
                dict_result = result._asdict()
                dict_result['request'] = result.request._asdict()
                return OrderSendResult(**dict_result)
            
            for deal in deals:
                if partial_volume > 0:
                    logger.info(f"Partial Closed Position: #{position.identifier} closed {close_volume}/{position.volume} {position.symbol} {position.price_open} closed at {deal.price} with profit {deal.profit} {self._get_account_currency()}")
                else:
                    logger.info(f"Closed Position: #{position.identifier} closed {close_volume} {position.symbol} {position.price_open} closed at {deal.price} with profit {deal.profit} {self._get_account_currency()}")
                
                # Generate and put the fill event
                self._generate_and_put_fill_event(trade_deal=deal, events_queue=self.events_queue)
        else:
            logger.warning(f"Closing Order for {position.symbol} failed with retcode: {result.retcode}. Last error: {mt5.last_error()}")

        # Convert the result to a dictionary and add the request as a dictionary too. Then convert it back to an OrderSendResult object
        dict_result = result._asdict()
        dict_result['request'] = result.request._asdict()

        # Return the OrderSendResult object. This is to ensure the result is always returned as an OrderSendResult object as defined in the our domain
        return OrderSendResult(**dict_result)

    def close_all_strategy_positions(self) -> None:
        for position in mt5.positions_get():
            if position.magic == int(self.magic_number):
                self.close_position(position.ticket)
    
    def close_all_strategy_positions_by_symbol(self, symbol: str) -> None:
        for position in mt5.positions_get(symbol=symbol):
            if position.magic == int(self.magic_number):
                self.close_position(position.ticket)

    def close_strategy_long_positions_by_symbol(self, symbol: str) -> None:
        for position in mt5.positions_get(symbol=symbol):
            if position.magic == int(self.magic_number) and position.type == mt5.ORDER_TYPE_BUY:
                self.close_position(position.ticket)

    def close_strategy_short_positions_by_symbol(self, symbol: str) -> None:
        for position in mt5.positions_get(symbol=symbol):
            if position.magic == int(self.magic_number) and position.type == mt5.ORDER_TYPE_SELL:
                self.close_position(position.ticket)

    def cancel_pending_order(self, order_ticket: int) -> OrderSendResult:
        """Cancel pending order"""
        # Check if order ticket is valid
        if order_ticket < 0:
            logger.error(f"Invalid order ticket: {order_ticket}")
            return False
        
        cancel_request = {
        'action': mt5.TRADE_ACTION_REMOVE,
        "order": order_ticket,
        }

        # Send the order request and get the result as a OrderSendResult object
        result = mt5.order_send(cancel_request)

        # Check if the order was executed successfully
        if self._check_succesful_order_execution(result):
            logger.info(f"Order {result.order} successfully cancelled")
        else:
            if result is None:
                logger.warning(f"Cancel Pending Order for ticket {order_ticket} failed. Result is None. Last error: {mt5.last_error()}")
                return None
            else:
                logger.warning(f"Cancel Pending Order for {result.request.symbol} failed with retcode: {result.retcode}. Last error: {mt5.last_error()}")

        # Convert the result to a dictionary and add the request as a dictionary too. Then convert it back to an OrderSendResult object
        dict_result = result._asdict()
        dict_result['request'] = result.request._asdict()

        # Return the OrderSendResult object. This is to ensure the result is always returned as an OrderSendResult object as defined in the our domain
        return OrderSendResult(**dict_result)

    def cancel_all_strategy_pending_orders(self) -> None:
        for order in mt5.orders_get():
            if order.magic == int(self.magic_number):
                self.cancel_pending_order(order.ticket)
    
    def cancel_all_strategy_pending_orders_by_type_and_symbol(self, order_type:str, symbol: str) -> None:
        """
        Cancels all specific type of pending orders from the strategy, in a specific symbol.
        Example: cancels all BUY_LIMIT orders in EURUSD.
        """
        order_type_int = Utils.order_type_str_to_int(order_type)
        if order_type_int == -1:
            logger.error(f"Invalid order type: {order_type}")
            return
        
        for order in mt5.orders_get(symbol=symbol):
            if order.magic == int(self.magic_number) and order.type == order_type_int:
                self.cancel_pending_order(order.ticket)
    
    def update_position_sl_tp(self, position_ticket: int, new_sl: float = 0.0, new_tp: float = 0.0) -> None:
        """Update position SL and TP"""
        # Check if position ticket is valid
        if position_ticket < 0:
            logger.error(f"Invalid order ticket: {position_ticket}")
            return False
        
        # Get the position we are referencing
        pos = mt5.positions_get(ticket=position_ticket)
        if not pos:
            logger.error(f"Position {position_ticket} not found")
            return
        
        position = pos[0]
        current_sl = position.sl
        current_tp = position.tp
        
        # Creating the trade request
        update_request = {
            'action': mt5.TRADE_ACTION_SLTP,
            'position': position_ticket,
            'sl': new_sl if new_sl != 0.0 else current_sl,
            'tp': new_tp if new_tp != 0.0 else current_tp,
        }

        # Send the order request and get the result as a OrderSendResult object
        result = mt5.order_send(update_request)

        # Check if the order was executed successfully
        if self._check_succesful_order_execution(result):
            logger.info(f"Position {position_ticket} SL/TP updated to {new_sl:.5f}/{new_tp:.5f}")
        else:
            logger.warning(f"Update Position SL/TP for {position_ticket} failed with retcode: {result.retcode}. Last error: {mt5.last_error()}")
    
    def _get_account_currency(self) -> str:
        """Get account currency"""
        return mt5.account_info()._asdict()['currency']

    def _get_account_balance(self) -> Decimal:
        """Get account balance in account currency"""
        return Decimal(str(mt5.account_info()._asdict()['balance']))

    def _get_account_equity(self) -> Decimal:
        """Get account equity in account currency"""
        return Decimal(str(mt5.account_info()._asdict()['equity']))

    def _get_account_floating_profit(self) -> Decimal:
        """Get account floating profit in account currency"""
        return Decimal(str(mt5.account_info()._asdict()['profit']))

    def _get_account_used_margin(self) -> Decimal:
        """Get account used margin in account currency"""
        return Decimal(str(mt5.account_info()._asdict()['margin']))

    def _get_account_free_margin(self) -> Decimal:
        """Get account free margin in account currency"""
        return Decimal(str(mt5.account_info()._asdict()['margin_free']))

    def _get_total_number_of_pending_orders(self) -> int:
        """Get total number of active pending orders"""
        return mt5.orders_total()

    def _get_strategy_pending_orders(self, symbol: str = '', ticket: int = None, group="") -> tuple[PendingOrder]:
        """Get current STRATEGY pending orders"""
        
        # Construct the arguments for the mt5.orders_get() function
        args = {}
        if symbol:
            args['symbol'] = symbol
        if ticket is not None:
            args['ticket'] = ticket
        if group:
            args['group'] = group
        
        # Returns a tuple of TradeOrder objects (or an empty tuple)
        orders = mt5.orders_get(**args)

        # Now we have a tuple of TradeOrder objects. We need to transform them into PendingOrder objects
        pending_orders = []
        for order in orders:
            if order.magic != self.magic_number:
                continue
            # Convert the TradeOrder object into a PendingOrder object
            pending_order = PendingOrder(price=Decimal(str(order.price_open)),          # In live mode, price_open is a float
                                        type="BUY_LIMIT" if order.type == 2 else "SELL_LIMIT" if order.type == 3 else "BUY_STOP" if order.type == 4 else "SELL_STOP" if order.type == 5 else "UNKNOWN",
                                        symbol=order.symbol,
                                        ticket=order.ticket,
                                        volume=Decimal(str(order.volume_current)),
                                        strategy_id=str(order.magic),
                                        sl=Decimal(str(order.sl)),
                                        tp=Decimal(str(order.tp)),
                                        comment=order.comment)
            pending_orders.append(pending_order)
        
        return tuple(pending_orders)

    def _get_total_number_of_positions(self) -> int:
        """Get total number of positions in the account"""
        return mt5.positions_total()

    def _get_strategy_positions(self, symbol: str = '', ticket: int = None, group="") -> tuple[OpenPosition]:
        """Gets current STRATEGY positions"""
        
        # Construct the arguments for the mt5.positions_get() function
        args = {}
        if symbol:
            args['symbol'] = symbol
        if ticket is not None:
            args['ticket'] = ticket
        if group:
            args['group'] = group
        
        # Getting a tuple of TradePosition objects
        positions = mt5.positions_get(**args)

        # Now we have a tuple of TradePosition objects. We need to transform them into OpenPosition objects
        open_positions = []
        for position in positions:
            if position.magic != self.magic_number:
                continue
            # Convert the TradePosition object into an OpenPosition object
            open_position = OpenPosition(time_entry=datetime.fromtimestamp(position.time_msc / 1000.0),
                                        price_entry=Decimal(str(position.price_open)),
                                        type="BUY" if position.type == 0 else "SELL",
                                        symbol=position.symbol,
                                        ticket=position.ticket,
                                        volume=Decimal(str(position.volume)),
                                        strategy_id=str(position.magic),
                                        unrealized_profit=Decimal(str(position.profit)),
                                        sl=Decimal(str(position.sl)),
                                        tp=Decimal(str(position.tp)),
                                        swap=Decimal(str(position.swap)),
                                        comment=position.comment)
            open_positions.append(open_position)
        
        return tuple(open_positions)

    def _get_symbol_min_volume(self, symbol: str) -> Decimal:
        """Get symbol min volume"""
        return Decimal(str(mt5.symbol_info(symbol).volume_min))