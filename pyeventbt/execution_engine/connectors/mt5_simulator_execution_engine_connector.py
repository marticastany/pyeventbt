"""
PyEventBT
Documentation: https://pyeventbt.com
GitHub: https://github.com/marticastany/pyeventbt

Author: Marti Castany
Copyright (c) 2025 Marti Castany
Licensed under the Apache License, Version 2.0
"""

from ..core.interfaces.execution_engine_interface import IExecutionEngine
from ..core.configurations.execution_engine_configurations import MT5SimulatedExecutionConfig
from pyeventbt.data_provider.core.interfaces.data_provider_interface import IDataProvider
from pyeventbt.broker.mt5_broker.core.entities.order_send_result import OrderSendResult
from pyeventbt.broker.mt5_broker.core.entities.trade_position import TradePosition
from pyeventbt.broker.mt5_broker.core.entities.trade_request import TradeRequest
from pyeventbt.broker.mt5_broker.core.entities.trade_order import TradeOrder
from pyeventbt.broker.mt5_broker.core.entities.trade_deal import TradeDeal
from pyeventbt.broker.mt5_broker.core.entities.account_info import AccountInfo
from pyeventbt.portfolio.core.entities.open_position import OpenPosition
from pyeventbt.portfolio.core.entities.pending_order import PendingOrder
from pyeventbt.events.events import BarEvent, FillEvent, OrderEvent, SignalType
from pyeventbt.broker.mt5_broker.mt5_simulator_wrapper import Mt5SimulatorWrapper as mt5
from pyeventbt.broker.mt5_broker.shared.shared_data import SharedData
from pyeventbt.utils.utils import Utils, ALL_FX_SYMBOLS

from datetime import datetime, timedelta, timezone
from queue import Queue
from decimal import Decimal
import logging

logger = logging.getLogger("pyeventbt")


class Mt5SimulatorExecutionEngineConnector(IExecutionEngine):

    # TODO: Add Check if margin is enough to keep positions opened. Maybe we need account leverage

    def __init__(self, configs: MT5SimulatedExecutionConfig, events_queue: Queue, data_provider: IDataProvider):
        """
        Initializes the MT5SimulatorExecutionEngineConnector object.

        Args:
        - events_queue (Queue): A queue object to store events.
        - data_handler (IDataHandler): An object that handles data.
        - initial_balance (float): The initial balance of the account.
        - account_currency (str): The currency of the account. Default is "EUR".
        - account_leverage (int): The leverage of the account. Default is 200.
        """

        self.events_queue = events_queue
        self.DATA_PROVIDER = data_provider

        # Data structures for holding the orders, positions and deals
        self.pending_orders: dict[int, OrderSendResult] = {} #list of OrderSendResult objects from the pending orders (all will have same MagicNumber)
        self.open_positions: dict[int, TradePosition] = {}  # dict with key = TradePosition.ticket and value = TradePosition object
        self.executed_deals: dict[int, TradeDeal] = {}  # dict with key = TradeDeal.ticket and value = TradeDeal object
        
        # Account values
        self.balance: Decimal = configs.initial_balance
        self.equity: Decimal = configs.initial_balance
        self.used_margin: Decimal = Decimal('0.0')
        self.free_margin: Decimal = self.equity - self.used_margin
        self.account_currency: str = configs.account_currency
        self.ticketing_counter: int = 200000000
        self.deal_ticketing_counter: int = 300000000
        self.margin_call = False

        # Update the shared data with the account information
        self._update_shared_data_account_info()

        # Symbols
        self.all_commodities_symbols = ("XAUUSD", "XAGUSD", "XTIUSD", "XNGUSD")
        self.all_indices_symbols = ("NI225", "WS30", "SP500", "FCHI40", "AUS200", "NDX", "UK100", "STOXX50E", "GDAXI", "SPA35")

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

    def _update_shared_data_account_info(self) -> None:
        """
        Updates the shared data with the account information.
        Returns:
        None
        """
        # SharedData.account_info.balance = self.balance
        # SharedData.account_info.equity = self.equity
        # SharedData.account_info.margin = self.used_margin
        # SharedData.account_info.margin_free = self.free_margin
        # SharedData.account_info.currency = self.account_currency

        # We are confident about the data types in backtest environment, so we can bypass pydantic validation for performance
        # Access the underlying __dict__ directly to bypass __setattr__ validation
        account_dict = SharedData.account_info.__dict__
        
        # Update values directly
        account_dict["balance"] = self.balance
        account_dict["equity"] = self.equity 
        account_dict["margin"] = self.used_margin
        account_dict["margin_free"] = self.free_margin
        account_dict["currency"] = self.account_currency

    def _compute_required_margin_for_order_in_account_currency(self, symbol: str, volume: float) -> float:
        """
        Computes the required margin for an order in the account currency.

        Args:
            symbol (str): The symbol of the financial instrument.
            volume (float): The volume of the order.

        Returns:
            float: The required margin for the order in the account currency.
        """
        # Get the relevant common data
        symbol_info = mt5.symbol_info(symbol)
        contract_size = symbol_info.trade_contract_size
        margin_rate = symbol_info.margin_initial
        margin_ccy = symbol_info.currency_margin
        
        # Margins will be given in margin_ccy of the symb
        if symbol in ALL_FX_SYMBOLS:
            # FX formula
            margin = volume * contract_size * margin_rate
        else:
            last_price = self.DATA_PROVIDER.get_latest_bid(symbol)
            # CFD formula
            margin = volume * contract_size * last_price * margin_rate
            
        # Convert the margin to the account currency
        return Utils.convert_currency_amount_to_another_currency(amount=margin, from_ccy=margin_ccy, to_ccy=self.account_currency, data_provider=self.DATA_PROVIDER)

    def _check_enough_money_in_account_to_execute_order(self, required_margin: Decimal) -> bool:
        # Check if there is enough money in the account to execute the order
        if self.free_margin < 0 or required_margin > self.free_margin:
            self.margin_call = True
            return False
        else:
            return True

    def _check_margin_call(self) -> None:
        if self.margin_call:
            logger.warning(f"MARGIN CALL. Terminating backtest. Free margin: {self.free_margin}, used margin: {self.used_margin}, equity: {self.equity}, balance: {self.balance}")
            self.DATA_PROVIDER.continue_backtest = False

    def _compute_trade_gross_profit_in_account_currency(self, symbol: str, entry_type:str, volume: Decimal, entry_price: Decimal, close_price: Decimal) -> Decimal:
        """
        Computes the profit of a trade in the account currency.

        Args:
            symbol (str): The symbol of the trade.
            volume (Decimal): The volume of the trade.
            entry_price (Decimal): The entry price of the trade.
            close_price (Decimal): The close price of the trade.

        Returns:
            Decimal: The profit of the trade in the account currency.
        """
        if entry_type not in ["BUY", "SELL"]:
            logger.error(f"Invalid position entry type: {entry_type}")
            return Decimal('0.0')
        
        symbol_info = mt5.symbol_info(symbol)
        # FX & CFD formula:  profit = (close_price - open_price) * volume * contract_size - in profit ccy
        profit = (close_price - entry_price) * volume * symbol_info.trade_contract_size

        if entry_type == "SELL":
            profit = -profit
        
        # Convert the profit to the account currency
        return Utils.convert_currency_amount_to_another_currency(amount=profit, from_ccy=symbol_info.currency_profit, to_ccy=self.account_currency, data_provider=self.DATA_PROVIDER)

    def _compute_commission_in_account_ccy(self, symbol: str, volume: Decimal, trade_price: Decimal) -> Decimal:
        """
        Calculates the fees of trading based on the Darwinex fee structure. Returns a POSITIVE value.

        COMMISSIONS:
            FX:
            notionalFX = 2.5; //Commission is 2.5 notional per side per lot. So 5.0 total roundtrip PER LOT.

            COMMODITIES:
            0.0025% del valor de la orden (web Darwinex)

            INDICES:
            NI225 = 35 JPY per lot
            WS30 = 0.35 USD per lot 
            SP500 = 0.275 points per lot
            The rest of indices = 2.75 points per lot
        """
        symbol_info = mt5.symbol_info(symbol)
        if symbol in ALL_FX_SYMBOLS:
            notional_commission = volume * Decimal('2.5')
        elif symbol in self.all_commodities_symbols:
            notional_commission = Decimal(str(symbol_info.trade_contract_size)) * volume * trade_price * Decimal('0.000025')
        elif symbol == "NI225":
            notional_commission = volume * 35
        elif symbol == "WS30":
            notional_commission = volume * Decimal('0.35')
        elif symbol == "SP500":
            notional_commission = volume * Decimal('0.275')
        elif symbol in ["FCHI40", "AUS200", "NDX", "UK100", "STOXX50E", "GDAXI", "SPA35"]:
            notional_commission = volume * Decimal('2.75')
        elif symbol == "SYNTHUSD":
            notional_commission = Decimal('0.0')
        else:
            # Those are the ETFs
            notional_commission = volume * Decimal('0.02')  # 0.02 USD per lot
        
        # Convert the notional commission to the account currency and return it as a positive value
        commission = Utils.convert_currency_amount_to_another_currency(amount=notional_commission, from_ccy=symbol_info.currency_margin, to_ccy=self.account_currency, data_provider=self.DATA_PROVIDER)
        return abs(commission)

    def _check_stop_loss_is_valid(self, signal_type: SignalType, sl: float, intended_fill_price: float) -> bool:
        """
        Check if the stop loss value is valid for the given order type and intended fill price.

        Args:
            signal_type (SignalType): The type of order (BUY, SELL).
            sl (float): The stop loss value to check.
            intended_fill_price (float): The intended fill price for the order.

        Returns:
            bool: True if the stop loss value is valid, False otherwise.
        """
        # If sl is 0.0, it means not being used, so it's a valid.
        if sl == 0.0:
            return True
        else:
            # If it's a buy, SL should be below the intended fill price
            if signal_type == "BUY":
                if sl <= float(intended_fill_price):  # TODO: remove the = part (only for testing new func for modfying current open position sl - not replicated in LIVE)
                    return True
                else:
                    return False
            # If it's a sell, SL should be above the intended fill price. If it's not a buy, it's a sell.
            else:
                if sl >= float(intended_fill_price):
                    return True
                else:
                    return False

    def _check_take_profit_is_valid(self, signal_type: SignalType, tp: float, intended_fill_price:float) -> bool:
        """
        Check if the take profit value is valid for the given order type and intended fill price.

        Args:
            signal_type (SignalType): The type of order (BUY, SELL).
            tp (float): The take profit value to check.
            intended_fill_price (float): The intended fill price for the order.

        Returns:
            bool: True if the take profit value is valid, False otherwise.
        """
        # If tp is 0.0, it means not being used, so it's a valid.
        if tp == 0.0:
            return True
        else:
            # If it's a buy, TP should be above the intended fill price
            if signal_type == "BUY":
                if tp > intended_fill_price:
                    return True
                else:
                    return False
            # If it's a sell, TP should be below the intended fill price
            else:
                if tp < intended_fill_price:
                    return True
                else:
                    return False

    def _check_desired_trade_volume_is_valid(self, order_event: OrderEvent) -> bool:
        """
        Check if the desired trade volume is valid for the given symbol.

        Args:
            symbol (str): The symbol of the financial instrument.
            volume (float): The desired trade volume.

        Returns:
            bool: True if the desired trade volume is valid, False otherwise.
        """
        symbol = order_event.symbol
        symbol_info = mt5.symbol_info(symbol)
        
        # Check that the desired trade volume can actually be traded (symbols have minimum values)
        if order_event.volume < symbol_info.volume_min:
            logger.warning(f"{order_event.time_generated} - Invalid volume: {order_event.volume}. It should be at least {symbol_info.volume_min}")
            return False
        else:
            return True

    def _check_if_sl_tp_hit(self, bar_event: BarEvent) -> None:
        """
        Checks if any stop loss or take profit has been hit in the symbol of the BarEvent and generates a fill event, for every order, if they have.
        
        """
        symbol = bar_event.symbol
        #denominator = Decimal(10 ** bar_event.data.digits)

        # We need to copy the open_positions dict because we will be deleting elements from it and we can't modify a dict while iterating over it
        iteration_open_positions = self.open_positions.copy()
        if len(iteration_open_positions) == 0:
            return
        
        for position in iteration_open_positions.values():
            if symbol != position.symbol or (position.sl == 0.0 and position.tp == 0.0):
                continue
            # To compute the hit of an SL or TP, we will be on the pessimistic side. First check SL (for if same bar H/L range), then TP.
            # Also, we will track Bid/Ask prices on the H/L (even if it is not the real spread at the H/L point) to track the hit.

            sl_hit = False
            tp_hit = False
            closed_price = 0.0
            # Check if SL has been hit. We first check SL as we don't have intra minute resolution and we assume a pessimistic approach.
            # Also, we check if SL has been surpassed by the "ask" (high + spread) for a sell position.
            if position.sl != Decimal("0.0"):
                high_f_ask = bar_event.data.high_f + bar_event.data.spread_f
                if (position.type == 0 and bar_event.data.low_f <= position.sl) or (position.type == 1 and high_f_ask >= position.sl):
                    sl_hit = True
                    closed_price = position.sl
            
            # Check if TP has been hit (in case of a sell, check the TP with the low price + the spread). For TP, we need a surpase, not an equal price.
            if not sl_hit and position.tp != Decimal("0.0"):
                low_f_ask = bar_event.data.low_f + bar_event.data.spread_f
                if (position.type == 0 and bar_event.data.high_f > position.tp) or (position.type == 1 and low_f_ask < position.tp):
                    tp_hit = True
                    closed_price = position.tp
                
            # If neither SL nor TP have been hit, continue to the next position
            if not sl_hit and not tp_hit:
                continue
            
            # STEP 1: delete the position from the open_positions dict
            try:
                del self.open_positions[position.ticket]
            except KeyError:
                logger.error(f"Error deleting position {position.ticket} from open_positions dict")
                continue

            # STEP 2: generate a FillEvent and put it in the event queue
            time_in_datetime: datetime = bar_event.datetime# + timedelta(minutes=1)  # The TP/SL would have been hit on the recently completed bar, which is the one we use to check with its H/L. The +1 min is only for mKT orders.
            #time_sec = int(time_in_datetime.timestamp())
            time_sec = int(time_in_datetime.replace(tzinfo=timezone.utc).timestamp())
            time_msc = int(time_sec * 1000)
            commission = self._compute_commission_in_account_ccy(symbol=symbol, volume=position.volume, trade_price=closed_price)
            entry_type = "BUY" if position.type == 0 else "SELL"
            profit = self._compute_trade_gross_profit_in_account_currency(symbol=symbol, entry_type=entry_type, volume=position.volume, entry_price=position.price_open, close_price=closed_price)
            
            fill_event = FillEvent(deal="OUT",
                                symbol=symbol,
                                time_generated=time_msc,
                                position_id=position.identifier,
                                strategy_id=str(position.magic),
                                exchange='MT5_SIM',
                                volume=position.volume,
                                price=closed_price,
                                signal_type='BUY' if position.type == 1 else 'SELL',   # 0 is buy, so if position was a sell (1), the exit is a buy
                                commission=commission,
                                swap=0.0,
                                fee=0.0,
                                gross_profit=profit,
                                ccy=self.account_currency)
            self.events_queue.put(fill_event)

            # Subtract the commission from the account balance
            self.balance -= commission
            
            # STEP 3: generate a TradeDeal of type out and add it to the executed_deals dict.
            # Because we are processing a deal out (a close) the deal order ticket is different from deal position id, so we have to add 1 to ticketing_counter.
            self.deal_ticketing_counter += 1
            self.ticketing_counter += 1

            deal_comment = f"[sl {position.sl}]" if sl_hit else f"[tp {position.tp}]"
            deal = TradeDeal(ticket=self.deal_ticketing_counter,
                            order=self.ticketing_counter,
                            time=time_sec,
                            time_msc=time_msc,
                            type=0 if position.type == 1 else 1,
                            entry=1,
                            magic=position.magic,
                            position_id=position.identifier,    # position id is the same for all deals that compose a single position
                            reason=4 if sl_hit else 5,  # 4 is SL, 5 is TP
                            volume=position.volume,
                            price=closed_price,
                            commission=-commission, # deals save the commission as negative
                            swap=0.0,
                            profit=profit,
                            fee=0.0,
                            symbol=symbol,
                            comment=deal_comment,
                            external_id="")
            # Add the deal to the executed_deals dict
            self.executed_deals[self.deal_ticketing_counter] = deal
    
            # Update account values: balance, used margin and free margin
            self.balance += profit
            #self.equity -= profit
            self.used_margin -= position.used_margin_acc_ccy
            self.free_margin = self.equity - self.used_margin

            close_reason = "STOP_LOSS" if sl_hit else "TAKE_PROFIT"
            signal_type = "BUY" if position.type == 0 else "SELL"
            # Should not add profit to the output comment, as it is the gross profit (takes spread into account) but not commissions
            logger.info(f"{time_in_datetime} - Closed Position: Position #{position.identifier} {signal_type} {deal.volume:.2f} {symbol} {position.price_open:.5f} closed at {deal.price:.5f} {close_reason} with profit {deal.profit:.2f} {self.account_currency}")

            # Update the shared data with the account information for the simulator
            self._update_shared_data_account_info()

    def _check_if_pending_orders_filled(self, bar_event: BarEvent) -> None:
        """
        Checks if any pending order have been filled and generates a fill event, for every order, if they have.
        """
        symbol = bar_event.symbol
        scale: int = 10 ** bar_event.data.digits

        # Remember pending_orders dict is a dict with key = OrderSendResult.order and value = OrderSendResult object

        # We need to copy the pending_orders dict because we will be deleting elements from it and we can't modify a dict while iterating over it
        iteration_pending_orders = self.pending_orders.copy()
        for order in iteration_pending_orders.values():
            if symbol != order.request.symbol:
                continue
            
            # Precompute the order price in the same scale as the bar event (integers)
            order_price_scaled = int(order.request.price * scale)
            high_ask = bar_event.data.high + bar_event.data.spread
            low_ask = bar_event.data.low + bar_event.data.spread

            # order.request.type --> 2 is buy limit, 3 is sell limit, 4 is buy stop, 5 is sell stop
            # Unifying conditions for order fill checks. We force price penetration, not only equality
            is_buy_limit_filled  = order.request.type == 2 and low_ask < order_price_scaled    # Tiene que penetrar el Ask
            is_sell_limit_filled = order.request.type == 3 and bar_event.data.high > order_price_scaled
            is_buy_stop_filled   = order.request.type == 4 and high_ask > order_price_scaled   # Tiene que penetrar el Ask
            is_sell_stop_filled  = order.request.type == 5 and bar_event.data.low < order_price_scaled

            # # If any condition is true, the order has been filled
            if is_buy_limit_filled or is_sell_limit_filled or is_buy_stop_filled or is_sell_stop_filled:
                # The order price has been penetrated, so we can proceed to check the fill and generate the fill event
                
                # STEP 1:Check if there is enough money in the account to execute the order
                required_margin = self._compute_required_margin_for_order_in_account_currency(symbol=symbol, volume=order.request.volume)
                if not self._check_enough_money_in_account_to_execute_order(required_margin=required_margin):
                    logger.warning(f"Not enough money for pending order {symbol} with {order.request.volume} lots to be filled. Required margin: {required_margin:.2f} {self.account_currency}, Free margin: {self.free_margin:.2f} {self.account_currency}")
                    continue
        
                # STEP 2: delete the order from the pending_orders dict
                try:
                    del self.pending_orders[order.order]
                except KeyError:
                    logger.error(f"Error deleting order {order.order} from pending_orders dict")
                    continue

                # STEP 3: Create a TradePosition object and add it to the open_positions dict. The TradePosition ticket is the same as the order ticket
                time_in_datetime: datetime = bar_event.datetime# + timedelta(minutes=1) # The +1 min is only for MKT orders, as the bar that activated the order is actually the one from the bar event (the last formed one)
                open_time_sec = int(time_in_datetime.replace(tzinfo=timezone.utc).timestamp())
                #open_time_sec = int(time_in_datetime.timestamp())
                open_time_msc = int(open_time_sec * 1000)
                
                # Fill price is the order request price, as we are checking if is the ASK that penetrated the order price in case of BUY limit or stop.
                fill_price = order.request.price
                
                position = TradePosition(ticket=order.order,
                                        time=open_time_sec,
                                        time_msc=open_time_msc,
                                        time_update=open_time_sec,
                                        time_update_msc=open_time_msc,
                                        type= 0 if order.request.type == 2 or order.request.type == 4 else 1, # 0 is buy, 1 is sell, 2 is buy limit, 4 is buy stop
                                        magic=order.request.magic,
                                        identifier=order.order,
                                        reason=3,
                                        volume=order.request.volume,
                                        price_open=fill_price,
                                        sl=order.request.sl,
                                        tp=order.request.tp,
                                        price_current=Decimal(bar_event.data.close) / scale,
                                        swap=0.0,
                                        profit=0.0,
                                        symbol=symbol,
                                        comment=order.request.comment,
                                        external_id="",
                                        used_margin_acc_ccy=required_margin)
                
                # Add the position to the open_positions dict
                self.open_positions[order.order] = position

                # STEP 4: update account values
                self.used_margin += required_margin
                self.free_margin = self.equity - self.used_margin
                
                # STEP 5: generate a FillEvent and put it in the event queue
                commission = self._compute_commission_in_account_ccy(symbol=symbol, volume=order.request.volume, trade_price=fill_price)
                fill_event = FillEvent(deal="IN",
                                symbol=symbol,
                                time_generated=open_time_msc,
                                position_id=position.identifier,
                                strategy_id=str(order.request.magic),
                                exchange='MT5_SIM',
                                volume=order.request.volume,
                                price=fill_price,
                                signal_type='BUY' if position.type == 0 else 'SELL',
                                commission=commission,
                                swap=0.0,
                                fee=0.0,
                                gross_profit=0.0,
                                ccy=self.account_currency)
                
                self.events_queue.put(fill_event)

                # Subtract the commission from the account balance
                self.balance -= commission
                
                # # STEP 6: generate a TradeDeal of type in and add it to the executed_deals dict
                self.deal_ticketing_counter += 1
                deal = TradeDeal(ticket=self.deal_ticketing_counter,
                                order= order.order,
                                time=open_time_sec,
                                time_msc=open_time_msc,
                                type=position.type,
                                entry=0,                    # deal IN, an entry
                                magic=order.request.magic,
                                position_id=position.identifier,
                                reason=3,
                                volume=order.request.volume,
                                price=fill_price,
                                commission=-commission, # deals save the commission as negative
                                swap=0.0,
                                profit=0.0,
                                fee=0.0,
                                symbol=order.request.symbol,
                                comment=order.request.comment,
                                external_id="")
                # TODO Maybe here we should add 1 to the order ticketing, as subsequent deals for the same position will have different order tickets
                # but same position_id
                
                # Add the deal to the executed_deals dict
                self.executed_deals[self.deal_ticketing_counter] = deal

                # order.request.type --> 2 is buy limit, 3 is sell limit, 4 is buy stop, 5 is sell stop
                order_type = "BUY_LIMIT" if order.request.type == 2 else "SELL_LIMIT" if order.request.type == 3 else "BUY_STOP" if order.request.type == 4 else "SELL_STOP"
                logger.info(f"{time_in_datetime} - Pending Order Filled: Order #{order.order} {order_type} {deal.volume} {symbol} at {deal.price:.5f} using {required_margin:.2f} {self.account_currency}")
        
            else:
                # If the order is not filled, continue to the next order
                continue
        
        # Update the shared data with the account information for the simulator
        self._update_shared_data_account_info()

    def _update_positions_floating_pnl_and_current_price(self, bar_event: BarEvent) -> None:
        """
        Updates the floating PnL (profit and loss) of the open positions for the symbol of the BarEvent.
        This method iterates through all open positions for the symbol of the BarEvent and updates their current price and profit.
        It then calculates the global PnL by summing up the profit of ALL open positions (across all symbols),
        mirroring how the MT5 platform computes equity in live trading.
        Finally, it updates the account values: equity and free margin.

        :param bar_event: The BarEvent object containing the symbol and data for the current bar.
        :type bar_event: BarEvent
        :return: None
        """
        # Update price and profit only for positions matching the bar's symbol
        for position in self.open_positions.values():
            if bar_event.symbol != position.symbol:
                continue

            # Update the current price of the position
            position.price_current = Decimal(str(bar_event.data.close_f))

            # Update the profit of the position
            entry_type = "BUY" if position.type == 0 else "SELL"
            position.profit = self._compute_trade_gross_profit_in_account_currency(symbol=position.symbol, entry_type=entry_type, volume=position.volume, entry_price=position.price_open, close_price=position.price_current)

        # Equity from ALL open positions, not just the current symbol. Other symbols carry
        # their last computed profit, which is the best available estimate — same as MT5 live
        # using the last known tick price per symbol.
        total_pnl = sum(p.profit for p in self.open_positions.values())
        self.equity = self.balance + total_pnl
        self.free_margin = self.equity - self.used_margin

        # Update the shared data with the account information for the simulator
        self._update_shared_data_account_info()

    def get_strategy_open_volume_by_symbol(self, symbol:str) -> Decimal:
        
        # Getting strategy positions in this symbol
        positions = self._get_strategy_positions(symbol=symbol)

        total_volume = Decimal(0)
        
        for position in positions:
            pos_vol = position.volume
            
            if position.type == "BUY":
                total_volume += pos_vol
            else:
                total_volume -= pos_vol
            
        return total_volume

    def _process_order_event(self, order_event: OrderEvent) -> None:

        if order_event.order_type == "MARKET":
            self._send_market_order(order_event)
        else:
            self._send_pending_order(order_event)

    def _update_values_and_check_executions_and_fills(self, bar_event: BarEvent) -> None:
        # First we check if any position has been closed (SL or TP hit)
        self._check_if_sl_tp_hit(bar_event) # updates balance, used_margin and free_margin
        self._update_positions_floating_pnl_and_current_price(bar_event) # updates equity and free_margin
        self._check_if_pending_orders_filled(bar_event) # uses freemargin. Updates used_margin, free_margin and balance(for the commission)
        self._check_margin_call()
        self._check_equity_balance_are_positive()

    def _check_equity_balance_are_positive(self) -> None:
        if self.equity < Decimal(0) or self.balance < Decimal(0):
            logger.warning(f"Equity or balance are negative. Equity: {self.equity}, Balance: {self.balance}. Terminating backtest.")
            self.DATA_PROVIDER.continue_backtest = False
    

    def get_smallest_long_strategy_position_by_symbol(self, symbol:str) -> tuple[int, Decimal] | None:
        positions = self._get_strategy_positions(symbol=symbol)
        
        # If there are positions we can perform the calc. If not we'll return None
        if positions:
            vol = Decimal(999999999999)
            target_ticket = 0

            for position in positions:
                if position.type == "BUY":
                    if position.volume < vol:
                        vol = position.volume
                        target_ticket = position.ticket
            
            # Could be that there are no LONG positions
            if vol == 999999999999 and target_ticket == 0:
                return None
            else:
                return target_ticket, vol
        else:
            return None

    def get_smallest_short_strategy_position_by_symbol(self, symbol:str) -> tuple[int, Decimal] | None:
        positions = self._get_strategy_positions(symbol=symbol)
        
        # If there are positions we can perform the calc. If not we'll return None
        if positions:
            vol = Decimal(999999999999)
            target_ticket = 0

            for position in positions:
                if position.type == "SELL":
                    if position.volume < vol:
                        vol = position.volume
                        target_ticket = position.ticket
            
            # Could be that there are no SHORT positions
            if vol == 999999999999 and target_ticket == 0:
                return None
            else:
                return target_ticket, vol
        else:
            return None

    def _send_market_order(self, order_event: OrderEvent) -> OrderSendResult:
        """
        Executes a Market Order and returns an OrderSendResult. Takes into account the spread.
        """
        # Get the symbol from the OrderEvent
        symbol = order_event.symbol
        time_in_datetime: datetime = order_event.time_generated
        magic = int(order_event.strategy_id)   #For mt5, strategy id must be made only with numbers
        comment = order_event.strategy_id + "-MKT"
        stop_loss = order_event.sl
        take_profit = order_event.tp

        # Check if the signal type is valid
        if not (order_event.signal_type == "BUY" or order_event.signal_type == "SELL"):
            logger.error(f"{time_in_datetime} - Invalid Signal Type : {order_event.signal_type} for a Market Order. Must be of BUY/SELL")
            return 0
        
        # Check that the desired trade volume can actually be traded (symbols have minimum values)
        if not self._check_desired_trade_volume_is_valid(order_event):
            return 0
        
        # Check the trade values are valid
        if not self._check_common_trade_values(volume=order_event.volume, stop_loss=stop_loss, take_profit=take_profit, magic=magic, deviation=0, comment=comment):
            return 0
        
        # Check if there is enough money in the account to execute the order
        required_margin = self._compute_required_margin_for_order_in_account_currency(symbol=symbol, volume=order_event.volume)
        if not self._check_enough_money_in_account_to_execute_order(required_margin=required_margin):
            logger.warning(f"{time_in_datetime} - Not enough money to execute {symbol} {order_event.volume} lots {order_event.signal_type} Market Order. Required margin: {required_margin:.2f} {self.account_currency}, Free margin: {self.free_margin:.2f} {self.account_currency}")
            return 0

        # STEP 1: simulate the execution: this is getting a price to be filled at and adding position to the open_positions dict
        fill_price = self.DATA_PROVIDER.get_latest_ask(symbol) if order_event.signal_type == "BUY" else self.DATA_PROVIDER.get_latest_bid(symbol)
        
        # Check if SL and TP are valid
        if not self._check_stop_loss_is_valid(signal_type=order_event.signal_type, sl=stop_loss, intended_fill_price=fill_price):
            logger.error(f"{time_in_datetime} - Market Order failed - Invalid STOP_LOSS: {stop_loss:.5f} for {order_event.signal_type} {symbol} at {fill_price:.5f}")
            return 0
        if not self._check_take_profit_is_valid(signal_type=order_event.signal_type, tp=take_profit, intended_fill_price=fill_price):
            logger.error(f"{time_in_datetime} - Market Order failed - Invalid TAKE_PROFIT: {take_profit:.5f} for {order_event.signal_type} {symbol} at {fill_price:.5f}")
            return 0

        # STEP 2: Create a TradePosition object and add it to the open_positions dict
        self.ticketing_counter += 1
        # the open time is not really the time of the bar event, but the time of the trade deal which happens 1 minute after the bar event time.
        #Represents that the trade opens at just the opening of the next bar, but we are using the last finished bar, so we add 1 minute to the bar event time
        #open_time_sec = int(time_in_datetime.timestamp())
        open_time_sec = int(time_in_datetime.replace(tzinfo=timezone.utc).timestamp())
        open_time_msc = int(open_time_sec * 1000)

        position = TradePosition(ticket=self.ticketing_counter,
                                time=open_time_sec,
                                time_msc=open_time_msc,
                                time_update=open_time_sec,
                                time_update_msc=open_time_msc,
                                type= 0 if order_event.signal_type == "BUY" else 1,
                                magic=magic,
                                identifier=self.ticketing_counter,
                                reason=3,
                                volume=order_event.volume,
                                price_open=fill_price,
                                sl=stop_loss,
                                tp=take_profit,
                                price_current=fill_price,
                                swap=0.0,
                                profit=0.0,
                                symbol=symbol,
                                comment=comment,
                                external_id="",
                                used_margin_acc_ccy=required_margin)
        
        # Add the position to the open_positions dict
        self.open_positions[self.ticketing_counter] = position
        
        # STEP 3: update account values
        self.used_margin += required_margin
        self.free_margin = self.equity - self.used_margin

        # STEP 4: generate a FillEvent and put it in the event queue
        commission = self._compute_commission_in_account_ccy(symbol=symbol, volume=order_event.volume, trade_price=fill_price)
        
        fill_event = FillEvent(deal="IN",
                                symbol=symbol,
                                time_generated=open_time_msc,
                                position_id=position.identifier,
                                strategy_id=order_event.strategy_id,
                                exchange='MT5_SIM',
                                volume=order_event.volume,
                                price=fill_price,
                                signal_type=order_event.signal_type,
                                commission=commission,
                                swap=0.0,
                                fee=0.0,
                                gross_profit=0.0,
                                ccy=self.account_currency)
        
        self.events_queue.put(fill_event)

        # Subtract the commission from the account balance
        self.balance -= commission
        
        # STEP 5: Generate a TradeDeal of type in and add it to the executed_deals dict
        self.deal_ticketing_counter += 1
        deal = TradeDeal(ticket=self.deal_ticketing_counter,
                        order= self.ticketing_counter,
                        time=open_time_sec,
                        time_msc=open_time_msc,
                        type=position.type,
                        entry=0,
                        magic=magic,
                        position_id=self.ticketing_counter,
                        reason=3,
                        volume=order_event.volume,
                        price=fill_price,
                        commission=-commission, # deals save the commission as negative
                        swap=0.0,
                        profit=0.0,
                        fee=0.0,
                        symbol=symbol,
                        comment=comment,
                        external_id="")
        # Add the deal to the executed_deals dict
        self.executed_deals[self.deal_ticketing_counter] = deal
        
        # STEP 6: create a OrderSendResult object with the result of the simulated order send
        request = TradeRequest(action=mt5.TRADE_ACTION_DEAL,
                                magic=magic,
                                order=0,
                                symbol=symbol,
                                volume=order_event.volume,
                                price=0.0,
                                stoplimit=0.0,
                                sl=stop_loss,
                                tp=take_profit,
                                deviation=0,
                                type=mt5.ORDER_TYPE_BUY if order_event.signal_type == "BUY" else mt5.ORDER_TYPE_SELL,
                                type_filling=mt5.ORDER_FILLING_FOK,
                                type_time=mt5.ORDER_TIME_GTC,
                                expiration=0,
                                comment=comment,
                                position=0,
                                position_by=0)
        
        result = OrderSendResult(retcode=10009,
                                deal=self.deal_ticketing_counter,
                                order=self.ticketing_counter,
                                volume=order_event.volume,
                                price=fill_price,
                                bid=fill_price,
                                ask=fill_price,
                                comment=comment,
                                request_id=0,
                                retcode_external=0,
                                request=request)
        
        logger.info(f"{time_in_datetime} - Market Order Filled: Order #{result.order} {order_event.signal_type} {order_event.volume:.2f} {symbol} at {fill_price:.5f} using {required_margin:.2f} {self.account_currency}")
        
        # Update the shared data with the account information for the simulator
        self._update_shared_data_account_info()

        return result

    def _send_pending_order(self, order_event: OrderEvent) -> OrderSendResult:
        """
        Sends a pending order to the broker
        """
        symbol = order_event.symbol
        time_in_datetime: datetime = order_event.time_generated
        magic = int(order_event.strategy_id)   #For mt5, strategy id must be made only with numbers
        comment = order_event.strategy_id + "-PDG"
        volume = order_event.volume
        price=order_event.order_price
        stop_loss = order_event.sl
        take_profit = order_event.tp

        # Check if the order type is valid
        if not (order_event.signal_type == "BUY" or order_event.signal_type == "SELL"):
            logger.error(f"{time_in_datetime} - Invalid Signal type: {order_event.signal_type} for a Pending Order. Must be of BUY/SELL")
            return 0
        
        # Check that the desired trade volume can actually be traded (symbols have minimum values)
        if volume < mt5.symbol_info(symbol).volume_min:
            logger.error(f"{time_in_datetime} - Invalid volume: {volume}. It should be at least {mt5.symbol_info(symbol).volume_min}")
            return 0
        
        # Check the trade values are valid
        if not self._check_common_trade_values(volume=volume, stop_loss=stop_loss, take_profit=take_profit, magic=magic, deviation=0, comment=comment):
            return 0
        
        # Check if SL and TP are valid
        if not self._check_stop_loss_is_valid(signal_type=order_event.signal_type, sl=stop_loss, intended_fill_price=price):
            logger.error(f"{time_in_datetime} - Pending Order failed - Invalid STOP_LOSS: {stop_loss:.5f} for {order_event.signal_type} {symbol} at {price:.5f}")
            return 0
        if not self._check_take_profit_is_valid(signal_type=order_event.signal_type, tp=take_profit, intended_fill_price=price):
            logger.error(f"{time_in_datetime} - Pending Order failed - Invalid TAKE_PROFIT: {take_profit:.5f} for {order_event.signal_type} {symbol} at {price:.5f}")
            return 0
        
        # Will need to generate an OrderSendResult
        self.ticketing_counter += 1
        # STEP 6: create a OrderSendResult object with the result of the simulated order send
        
        if order_event.signal_type == "BUY":
            if order_event.order_type == "LIMIT":   
                mt5_order_type = 2
            else: # meaning "STOP"
                mt5_order_type = 4

        else: # meanin "SELL"
            if order_event.order_type == "LIMIT":
                mt5_order_type = 3
            else: # meaning "STOP"
                mt5_order_type = 5
        
        request = TradeRequest(action=mt5.TRADE_ACTION_PENDING,
                                magic=magic,
                                order=0,
                                symbol=symbol,
                                volume=volume,
                                price=price,
                                stoplimit=0.0,
                                sl=stop_loss,
                                tp=take_profit,
                                deviation=0,
                                type=mt5_order_type,
                                type_filling=mt5.ORDER_FILLING_FOK,
                                type_time=mt5.ORDER_TIME_GTC,
                                expiration=0,
                                comment=comment,
                                position=0,
                                position_by=0)
        
        result = OrderSendResult(retcode=10009,   # With the previous checks we avoid the most common errors, so we can assume the order will be sent
                                deal=0,   # Pending orders don't generate deals
                                order=self.ticketing_counter,
                                volume=volume,
                                price=0.0,
                                bid=0.0,
                                ask=0.0,
                                comment='Request executed',
                                request_id=0,
                                retcode_external=0,
                                request=request)

        # Add the pending order to the pending_orders dict
        self.pending_orders[self.ticketing_counter] = result

        # Update the shared data with the account information for the simulator
        self._update_shared_data_account_info()
        
        if stop_loss == Decimal('0.0') and take_profit == Decimal('0.0'):
            logger.info(f"{time_in_datetime} - Pending Order Sent: Order #{result.order} {order_event.signal_type} {order_event.order_type} {volume} {symbol} at {price:.5f}")
        else:
            logger.info(f"{time_in_datetime} - Pending Order Sent: Order #{result.order} {order_event.signal_type} {order_event.order_type} {volume} {symbol} at {price:.5f} SL: {stop_loss:.5f} TP: {take_profit:.5f}")

        return result

    def cancel_pending_order(self, order_ticket: int) -> OrderSendResult:
        """
        Cancels a specific pending order

        Args:
        - order_ticket (int): The ticket number of the order to be cancelled

        Returns:
        - OrderSendResult: An OrderSendResult object with the result of the operation
        """
            # Check if the order exists in the pending orders list
        if order_ticket in self.pending_orders:

            # Create a TradeRequest object for the cancellation
            request = TradeRequest(
                action=mt5.TRADE_ACTION_REMOVE,
                magic=0,  # order.request.magic,
                order=order_ticket,
                symbol='',
                volume=0.0,
                price=0.0,
                stoplimit=0.0,
                sl=0.0,
                tp=0.0,
                deviation=0,
                type=0,
                type_filling=0,
                type_time=0,
                expiration=0,
                comment='',
                position=0,
                position_by=0
            )
            
            # Create an OrderSendResult object with the result of the operation
            result = OrderSendResult(
                retcode=10009,  # With the previous checks we avoid the most common errors, so we can assume the order will be sent
                deal=0,  # Pending orders don't generate deals
                order=order_ticket,
                volume=0.0,
                price=0.0,
                bid=0.0,
                ask=0.0,
                comment='Request executed',
                request_id=0,
                retcode_external=0,
                request=request
            )

            time_in_datetime: datetime = self.DATA_PROVIDER.get_latest_datetime(self.pending_orders[order_ticket].request.symbol) + timedelta(minutes=1)
            
            # Remove the order from the pending_orders dict
            del self.pending_orders[order_ticket]
            
            logger.info(f"{time_in_datetime} - Pending Order #{order_ticket} has been cancelled")
            return result
        
        else:
            # If the order does not exist, return an error result
            logger.warning(f"Pending Order #{order_ticket} not found")
            return OrderSendResult(
                retcode=10011,  # TRADE_RETCODE_ERROR
                deal=0,
                order=order_ticket,
                volume=0.0,
                price=0.0,
                bid=0.0,
                ask=0.0,
                comment='Order not found',
                request_id=0,
                retcode_external=0,
                request=None
                )
    
    def cancel_all_strategy_pending_orders(self) -> None:
        """
        Cancels all pending orders for the strategy
        """
        for order in self._get_strategy_pending_orders():
            self.cancel_pending_order(order.ticket)
    
    def cancel_all_strategy_pending_orders_by_type_and_symbol(self, order_type:str, symbol: str) -> None:
        """
        Cancels all pending orders for the strategy with a specific order type and symbol
        """

        order_type_int = Utils.order_type_str_to_int(order_type)
        if order_type_int == -1:
            logger.error(f"Invalid order type: {order_type}")
            return
        
        for order in self._get_strategy_pending_orders():
            if order.type == order_type and order.symbol == symbol:
                self.cancel_pending_order(order.ticket)

    def close_position(self, position_ticket: int, partial_volume:Decimal = Decimal('0.0')) -> OrderSendResult:
        """
        Closes a currently opened position. Also allows partial close if partial_volume is passed
        """
        # Get the position from the open_positions dict (NEW WAY TO AVOID THE LOOP)
        try:
            position = self.open_positions[position_ticket]
        except KeyError:
            logger.error(f"Error closing position {position_ticket}. It doesn't exist in open_positions dict")
            return 0
    
        # STEP 1: delete the position from the open_positions dict if not a partial close. If partial close, reduce the volume of the position
        try:
            if partial_volume > 0:
                self.open_positions[position_ticket].volume = position.volume - partial_volume
            else:
                del self.open_positions[position_ticket]  # we can delete it as now we are not iterating over the dict
        except KeyError:
            logger.error(f"Error deleting position {position_ticket} from open_positions dict OR updating partial close - partial_volume: {partial_volume}")
            return 0

        # Defining the volume we need to close to account for a partial close
        volume_to_close = partial_volume if partial_volume > 0 else position.volume

        # If position is a buy (0), the closing (selling) will be at the bid. If position is a sell (1), closing will be at the ask
        last_tick = self.DATA_PROVIDER.get_latest_tick(position.symbol)
        close_price: Decimal = last_tick['ask'] if position.type == 1 else last_tick['bid']
        
        # STEP 2: generate a FillEvent and put it in the event queue
        time_in_datetime: datetime = self.DATA_PROVIDER.get_latest_datetime(position.symbol) + timedelta(minutes=1)
        #time_sec = int(time_in_datetime.timestamp())
        time_sec = int(time_in_datetime.replace(tzinfo=timezone.utc).timestamp())
        time_msc = int(time_sec * 1000)
        commission = self._compute_commission_in_account_ccy(symbol=position.symbol, volume=volume_to_close, trade_price=close_price)
        direction = 'BUY' if position.type == 1 else 'SELL' # if position type is 1 (sell), the closing will be a buy
        entry_type = "BUY" if position.type == 0 else "SELL"
        profit = self._compute_trade_gross_profit_in_account_currency(symbol=position.symbol, entry_type=entry_type, volume=volume_to_close, entry_price=position.price_open, close_price=close_price)
        
        fill_event = FillEvent(deal="OUT",
                            symbol=position.symbol,
                            time_generated=time_msc,
                            position_id=position.identifier,
                            strategy_id=str(position.magic),
                            exchange='MT5_SIM',
                            volume=volume_to_close,
                            price=close_price,
                            signal_type=direction,
                            commission=commission,
                            swap=0.0,
                            fee=0.0,
                            gross_profit=profit,
                            ccy=self.account_currency)
        
        self.events_queue.put(fill_event)  

        # Subtract the commission from the account balance
        self.balance -= commission      
        
        # STEP 3: generate a TradeDeal of type out and add it to the executed_deals dict.
        # Because we are processing a deal out (a close) the deal.order ticket is different from deal.position_id, so we have to add 1 to ticketing_counter.
        # and also the deal.ticket itself is different so we als odd 1 to deal_ticketing_counter
        self.deal_ticketing_counter += 1
        self.ticketing_counter += 1

        deal = TradeDeal(ticket=self.deal_ticketing_counter,
                        order=self.ticketing_counter,
                        time=time_sec,
                        time_msc=time_msc,
                        type=0 if position.type == 1 else 1,
                        entry=1, # deal OUT, a close
                        magic=position.magic,
                        position_id=position.identifier,
                        reason=3, # 3 is expert advisor
                        volume=volume_to_close,
                        price=close_price,
                        commission=-commission, # deals save the commission as negative
                        swap=0.0,
                        profit=profit,
                        fee=0.0,
                        symbol=position.symbol,
                        comment="",
                        external_id="")
        # Add the deal to the executed_deals dict
        self.executed_deals[self.deal_ticketing_counter] = deal

        # STEP 4: update account values
        self.balance += profit
        self.used_margin -= position.used_margin_acc_ccy
        self.free_margin = self.equity - self.used_margin

        # STEP 5: create a OrderSendResult object with the result of the simulated order send
        request = TradeRequest(action=mt5.TRADE_ACTION_DEAL,
                                magic=position.magic,
                                order=0,
                                symbol=position.symbol,
                                volume=volume_to_close,
                                price=0.0,
                                stoplimit=0.0,
                                sl=0.0,
                                tp=0.0,
                                deviation=0,
                                type=mt5.ORDER_TYPE_BUY if position.type == 1 else mt5.ORDER_TYPE_SELL,
                                type_filling=mt5.ORDER_FILLING_FOK,
                                type_time=mt5.ORDER_TIME_GTC,
                                expiration=0,
                                comment="",
                                position=position.ticket,
                                position_by=0)
        
        result = OrderSendResult(retcode=10009,
                                deal=self.deal_ticketing_counter,
                                order=self.ticketing_counter,
                                volume=volume_to_close,
                                price=close_price,
                                bid=close_price,
                                ask=close_price,
                                comment='Request executed',
                                request_id=0,
                                retcode_external=0,
                                request=request)
        
        # Update the shared data with the account information for the simulator
        self._update_shared_data_account_info()
        
        if partial_volume > 0:
            logger.info(f"{time_in_datetime} - Partial Closed Position: Position #{position.identifier} closed {deal.volume:.2f}/{position.volume:.2f} {position.symbol} {position.price_open:.5f} closed at {deal.price:.5f} with profit {deal.profit:.2f} {self.account_currency}")
        else:
            logger.info(f"{time_in_datetime} - Closed Position: Position #{position.identifier} {direction} {deal.volume:.2f} {position.symbol} {position.price_open:.5f} closed at {deal.price:.5f} with profit {deal.profit:.2f} {self.account_currency}")

        return result

    def close_all_strategy_positions(self) -> None:
        """
        Closes all the positions of the strategy
        """
        # Get all the positions of the strategy
        positions = self._get_strategy_positions()
        for position in positions:
            self.close_position(position_ticket=position.ticket)

    def close_strategy_long_positions_by_symbol(self, symbol: str) -> None:
        """
        Closes all the long positions of the strategy for a specific symbol
        """
        # Get all the positions of the strategy
        positions = self._get_strategy_positions(symbol=symbol)
        for position in positions:
            if position.type == "BUY":   # OpenPosition objects type are strings "BUY" or "SELL"
                self.close_position(position_ticket=position.ticket)

    def close_strategy_short_positions_by_symbol(self, symbol: str) -> None:
        """
        Closes all the short positions of the strategy for a specific symbol
        """
        # Get all the positions of the strategy
        positions = self._get_strategy_positions(symbol=symbol)
        for position in positions:
            if position.type == "SELL":   # OpenPosition objects type are strings "BUY" or "SELL"
                self.close_position(position_ticket=position.ticket)

    def update_position_sl_tp(self, position_ticket: int, new_sl: float = 0.0, new_tp: float = 0.0) -> None:
        """
        Updates the Stop Loss and Take Profit of a position
        """
        # Get the position from the open_positions dict
        position = None
        for pos in self.open_positions.values():
            if pos.ticket == position_ticket:
                position = pos
                break
        
        if position is None:
            return
        
        tick = self.DATA_PROVIDER.get_latest_tick(position.symbol)
        time_in_datetime = datetime.fromtimestamp(tick['time'])

        # if position is None:
        #     logger.error(f"{time_in_datetime} - Error updating position {position_ticket}. It doesn't exist in open_positions dict")
        #     return 0
        
        # Check if the new SL and TP are valid
        if not self._check_stop_loss_is_valid(signal_type="BUY" if position.type == 0 else "SELL", sl=new_sl, intended_fill_price=position.price_current):
            logger.error(f"{time_in_datetime} - Invalid STOP_LOSS: {new_sl:.5f} for {position.symbol} at {position.price_open:.5f}")
            new_sl = Decimal('0.0')
        
        # Here we pass price_current as you can place a TP below the entry price, but only if it's also above the CURRENT price (so in a losing positon in this case)
        if not self._check_take_profit_is_valid(signal_type="BUY" if position.type == 0 else "SELL", tp=new_tp, intended_fill_price=position.price_current):
            logger.error(f"{time_in_datetime} - Invalid TAKE_PROFIT: {new_tp:.5f} for {position.symbol} at {position.price_open:.5f}")
            new_tp = Decimal('0.0')
        
        # Update the position's sl and tp
        if new_sl != 0.0:
            position.sl = Decimal(new_sl)
            logger.info(f"{time_in_datetime} - Position {position_ticket} SL updated to {new_sl:.5f}")
        if new_tp != 0.0:
            position.tp = Decimal(new_tp)
            logger.info(f"{time_in_datetime} - Position {position_ticket} TP updated to {new_tp:.5f}")

    def _get_account_currency(self) -> str:
        """Get account currency"""
        return self.account_currency

    def _get_account_balance(self) -> float:
        """Get account balance in account currency"""
        return self.balance

    def _get_account_equity(self) -> float:
        """Get account equity in account currency"""
        return self.equity

    def _get_account_floating_profit(self) -> float:
        """Get account floating profit in account currency"""
        return self.equity - self.balance

    def _get_account_used_margin(self) -> float:
        """Get account used margin in account currency"""
        return self.used_margin

    def _get_account_free_margin(self) -> float:
        """Get account free margin in account currency"""
        return self.free_margin

    def _get_total_number_of_pending_orders(self) -> int:
        """Get total number of active pending orders"""
        #Get the elements in pending_orders dict
        return len(self.pending_orders)

    def _get_strategy_pending_orders(self, symbol: str = '', ticket: int = None, group="") -> tuple[PendingOrder]:
        """Get pending orders. The TradeOrder returned has some missing data"""
        #Get the elements in pending_orders dict and convert them into a tuple of TradeOrder objects
        if group != "":
            logger.error(f"(get_pending_orders MT5 SIM) - 'Group' {group} not supported yet")
            return ()
        
        elements = ()
        if ticket is None:
            elements = tuple(self.pending_orders.values())
        else:
            elements = tuple(self.pending_orders[ticket])  # Tuple of only one OrderSendResult object
        
        if symbol != '':
            # Filter by symbol. Elements already filled depending on ticket
            #elements = tuple(filter(lambda x: x.request.symbol == symbol, elements))
            elements = tuple(x for x in elements if x.request.symbol == symbol)  # Faster
        
        # At this point we have a tuple of OrderSend objects. Need to transform into PendingOrder objects
        order_list = []
        for order in elements:
            pending_order = PendingOrder(price=order.request.price,
                                        type=Utils.order_type_int_to_str(order.request.type),
                                        symbol=order.request.symbol,
                                        ticket=order.order,
                                        volume=order.request.volume,
                                        strategy_id=str(order.request.magic),
                                        sl=order.request.sl,
                                        tp=order.request.tp,
                                        comment=order.request.comment)
            
            order_list.append(pending_order)
        
        return tuple(order_list)

    def _get_total_number_of_positions(self) -> int:
        """Get total number of active opened positions"""
        return len(self.open_positions)

    def _get_positions_mt5_format(self, symbol: str = '', ticket: int = None, group="") -> tuple:
        """Get current positions and returns a tuple of TradePosition objects"""
        if group != "":
            logger.error(f"(get_pending_orders MT5 SIM) - 'Group' {group} not supported yet")
            return ()
        
        # Get the elements in open_positions dict. They are already TradePosition objects
        elements = ()
        if ticket is None:
            elements = tuple(self.open_positions.values())
        else:
            elements = tuple(self.open_positions[ticket])  # Tuple of only one OrderSendResult object
        
        if symbol != '':
            # Filter by symbol. Elements is already filled depending on ticket
            #elements = tuple(filter(lambda x: x.request.symbol == symbol, elements))
            elements = tuple(x for x in elements if x.request.symbol == symbol)  # Faster
        
        # At this point we have a tuple of TradePosition objects, which is exactly what we want.
        # This works as if it was MT5 itself, but ideally we now would like to receive open_positions as defined in our DOMAIN
        return elements
    
    def _get_strategy_positions(self, symbol: str = '', ticket: int = None) -> tuple[OpenPosition]:
        """
        Get current SRATEGY positions portfolio
        As it is for the backtesting engine, no need to filter by magic number. All positions are from the same strategy
        """
        # It will return a tuple of OpenPosition objects
        # Get the positions in open_positions dict. They are already TradePosition objects
        positions = ()
        if ticket is None:
            positions = tuple(self.open_positions.values())
        else:
            positions = tuple(self.open_positions[ticket])  # Tuple of only one TradePosition object
        
        if symbol != '':
            # Filter by symbol. Positions is already filled depending on ticket
            #positions = tuple(filter(lambda x: x.symbol == symbol, positions))
            positions = tuple(x for x in positions if x.symbol == symbol)  # Faster
        
        # Now we have a tuple of TradePosition objects. We need to transform them into OpenPosition objects
        open_positions = []
        for position in positions:
            # Convert the TradePosition object into an OpenPosition object
            open_position = OpenPosition(time_entry=datetime.fromtimestamp(position.time_msc / 1000.0),
                                                price_entry=position.price_open,
                                                type="BUY" if position.type == 0 else "SELL",
                                                symbol=position.symbol,
                                                ticket=position.ticket,
                                                volume=position.volume,
                                                strategy_id=str(position.magic),
                                                unrealized_profit=position.profit,
                                                sl=position.sl,
                                                tp=position.tp,
                                                swap=position.swap,
                                                comment=position.comment)
            open_positions.append(open_position)
        
        return tuple(open_positions)
    
    def _get_symbol_min_volume(self, symbol: str) -> Decimal:
        """Get symbol min volume"""
        return mt5.symbol_info(symbol).volume_min
