"""
PyEventBT
Documentation: https://pyeventbt.com
GitHub: https://github.com/marticastany/pyeventbt

Author: Marti Castany
Copyright (c) 2025 Marti Castany
Licensed under the Apache License, Version 2.0
"""

from pyeventbt.events.events import BarEvent, Bar
from ..core.interfaces.data_provider_interface import IDataProvider
from ..core.configurations.data_provider_configurations import MT5LiveDataConfig
from decimal import Decimal

#from queue import Queue
from datetime import datetime, timezone
import pandas as pd

import polars as pl

from pyeventbt.utils.utils import check_platform_compatibility
#from pyeventbt.utils.utils import Utils

import logging

logger = logging.getLogger("pyeventbt")

try:
    if check_platform_compatibility(raise_exception=False):
        import MetaTrader5 as mt5
    else:
        mt5 = None
except ImportError:
    mt5 = None

class Mt5LiveDataProvider(IDataProvider):
    """
    This class is designed to provide real data from an MT5 terminal data feed
    and provide an interface to obtain the "latest" bar.
    """
    def __init__(self, configs: MT5LiveDataConfig):
        """
        Initialises the data handler by requesting the MT5 terminal to connect to the data feed and
        downloading the historical data for the symbols in the symbol list.
        """
        #self.event_queue = event_queue
        self.symbol_list = list(dict.fromkeys(configs.tradeable_symbol_list))
        if len(self.symbol_list) != len(configs.tradeable_symbol_list):
            duplicates = list(dict.fromkeys(s for s in configs.tradeable_symbol_list if configs.tradeable_symbol_list.count(s) > 1))
            logger.warning(f"You had duplicated symbols in your symbol list: {duplicates}. Duplicates have been automatically removed — no action needed.")
        self.timeframes_list = configs.timeframes_list

        # We need to store the datetime of the last bar seen for each symbol so we can generate the events with update_bars()
        self.last_bar_datetime: dict[str, datetime] = {symbol: datetime.min for symbol in self.symbol_list}

        # {'EURUSD': {'1min': datetime, '5min':datetime}, 'USDJPY':{...}}
        self.last_bar_tf_datetime: dict[str, dict[str, datetime]] = {symbol: {timeframe: datetime.min.replace(tzinfo=timezone.utc) for timeframe in self.timeframes_list} for symbol in self.symbol_list}

        # Create a tuple of main futures contract names
        self.futures_tuple = ("KE", "ZC", "ZL", "ZM", "ZS", "HE", "LE", "MBT", "BZ", "CL", "HO", "NG", "RB", "GC", "HG", "SI", "PL", "6A", "6B", "6C", "6E", "6J", "6N", "6S", "ES", "NQ", "RTY", "YM", "FDAX", "FESX", "ZN", "FGBL")


    def _map_timeframe(self, timeframe: str) -> int:

        try:
            timeframe_mapping = {
                '1min': mt5.TIMEFRAME_M1,
                '2min': mt5.TIMEFRAME_M2,
                '3min': mt5.TIMEFRAME_M3,
                '4min': mt5.TIMEFRAME_M4,
                '5min': mt5.TIMEFRAME_M5,
                '6min': mt5.TIMEFRAME_M6,
                '10min': mt5.TIMEFRAME_M10,
                '12min': mt5.TIMEFRAME_M12,
                '15min': mt5.TIMEFRAME_M15,
                '20min': mt5.TIMEFRAME_M20,
                '30min': mt5.TIMEFRAME_M30,
                '1h': mt5.TIMEFRAME_H1,
                '1H': mt5.TIMEFRAME_H1,
                '2h': mt5.TIMEFRAME_H2,
                '2H': mt5.TIMEFRAME_H2,
                '3h': mt5.TIMEFRAME_H3,
                '3H': mt5.TIMEFRAME_H3,
                '4h': mt5.TIMEFRAME_H4,
                '4H': mt5.TIMEFRAME_H4,
                '6h': mt5.TIMEFRAME_H6,
                '6H': mt5.TIMEFRAME_H6,
                '8h': mt5.TIMEFRAME_H8,
                '8H': mt5.TIMEFRAME_H8,
                '12h': mt5.TIMEFRAME_H12,
                '12H': mt5.TIMEFRAME_H12,
                '1D': mt5.TIMEFRAME_D1,
                '1W': mt5.TIMEFRAME_W1,
                '1M': mt5.TIMEFRAME_MN1
            }
        
        except KeyError:
            logger.error(f"Timeframe '{timeframe}' is not valid.")
            raise
        else:
            return timeframe_mapping[timeframe]
    
    def get_latest_bar_old(self, symbol: str, timeframe: str = '1min') -> pd.Series:
        """
        Returns the latest bar for a given symbol and timeframe.

        Args:
            symbol (str): The symbol to retrieve the latest bar for.
            timeframe (str, optional): The timeframe to retrieve the latest bar for. Defaults to '1min'.

        Returns:
            pd.Series: The latest bar for the given symbol and timeframe.
        """
        # Define to get data from the last closed (formed) bar and the timeframe
        from_pos = 1
        bars_count = 1
        tf = self._map_timeframe(timeframe)        
        
        # Get the latest bar in a dataframe and process it for returning a series
        try:
            bars = pd.DataFrame(mt5.copy_rates_from_pos(symbol, tf, from_pos, bars_count))
            if bars is None:
                logger.error(f"Symbol '{symbol}' is not available in the historical data set.")
                raise

            if bars.empty:
                return pd.Series()
            
            # Convert the timestamp to datetime and set it as index
            bars['time'] = pd.to_datetime(bars['time'], unit='s')    #TODO: CHECK TIMEZONE - OK, IS IN MT5 SERVER TIME
            bars.set_index('time', inplace=True)
            bars.rename(columns={'tick_volume': 'tickvol', 'real_volume': 'volume'}, inplace=True)
            bars = bars[['open', 'high', 'low', 'close', 'tickvol', 'volume', 'spread']]

        except:
            logger.error(f"Could not get latest bar for symbol '{symbol}' and timeframe '{timeframe}'")
            raise
        else:
            if bars.shape[0] == 0:
                return pd.Series()
            else:
                return bars.iloc[-1]

    def get_latest_bar(self, symbol: str, timeframe: str = "1min") -> BarEvent | None:
        """
        Returns the latest bar as a BarEvent.
        """
        if not symbol or not isinstance(symbol, str):
            logger.error("Invalid symbol provided")
            return None
    
        from_pos   = 1
        bars_count = 1
        tf         = self._map_timeframe(timeframe)

        try:
            # Request data from MT5
            records = mt5.copy_rates_from_pos(symbol, tf, from_pos, bars_count)
            if not records:
                logger.error(f"No data for {symbol} @ {timeframe}")
                return None

            r = records[0]
            
            # Fetch symbol digits (precision):
            sym_info = mt5.symbol_info(symbol)
            if sym_info is None:
                # Fallback precision detection
                digits = 3 if "JPY" in symbol else 5
                logger.warning(f"Using fallback precision for {symbol}: {digits} digits")
            else:
                digits = sym_info.digits

            # Create properly scaled integers
            scale = 10**digits
            bar = Bar(
                open    = int(r["open"] * scale),
                high    = int(r["high"] * scale),
                low     = int(r["low"] * scale),
                close   = int(r["close"] * scale),
                tickvol = int(r["tick_volume"]),
                volume  = int(r["real_volume"]),
                spread  = int(r["spread"]),
                digits  = digits
            )
            
            # Timestamp is in seconds since epoch, MT5 server time:
            dt = datetime.fromtimestamp(r["time"], tz=timezone.utc)

            return BarEvent(
                symbol    = symbol,
                datetime  = dt,
                data      = bar,
                timeframe = timeframe
            )

        except KeyError as e:
            logger.error(f"Missing data field in MT5 response for {symbol}: {e}")
            return None
        except ValueError as e:
            logger.error(f"Value error processing {symbol} data: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting latest bar for {symbol} @ {timeframe}: {str(e)}")
            return None


    def get_latest_bars_old_pandas(self, symbol: str, timeframe: str = '1min', N:int = 2) -> pd.DataFrame:
        """
        Returns the latest N bars of historical data for a given symbol and timeframe.
        
        Args:
        symbol (str): The symbol to retrieve data for.
        timeframe (str): The timeframe to retrieve data for. Defaults to '1min'.
        N (int): The number of bars to retrieve. Defaults to 2.
        
        Returns:
        pd.DataFrame: A DataFrame containing the latest N bars of historical data for the given symbol and timeframe.
        """
        # Define to get data from the last closed (formed) bar and the timeframe
        from_pos = 1
        bars_count = N if N > 0 else 2
        tf = self._map_timeframe(timeframe)
        
        # Get the latest N bars in a dataframe and process it for returning a series
        try:
            bars = pd.DataFrame(mt5.copy_rates_from_pos(symbol, tf, from_pos, bars_count))
            if bars is None:
                logger.error(f"Symbol '{symbol}' is not available in the historical data set.")
                raise

            if bars.empty:
                return pd.DataFrame()
            
            # Convert the timestamp to datetime and set it as index
            bars['time'] = pd.to_datetime(bars['time'], unit='s')
            bars.set_index('time', inplace=True)
            bars.rename(columns={'tick_volume': 'tickvol', 'real_volume': 'volume'}, inplace=True)
            bars = bars[['open', 'high', 'low', 'close', 'tickvol', 'volume', 'spread']]

        except:
            logger.error(f"Could not get latest bars for symbol '{symbol}' and timeframe '{timeframe}'")
            raise
        else:
            return bars
    
    def get_latest_bars(self, symbol: str, timeframe: str = '1min', N: int = 2) -> pl.DataFrame | None:
        """
        Returns the latest N bars as a Polars DataFrame.

        Args:
            symbol (str): The symbol to retrieve data for.
            timeframe (str): The timeframe to retrieve data for. Defaults to '1min'.
            N (int): The number of bars to retrieve. Defaults to 2.
        Returns:
            pl.DataFrame: A Polars DataFrame containing the latest N bars of historical data for the given symbol and timeframe.
        """
        if not symbol or not isinstance(symbol, str):
            logger.error("Invalid symbol provided")
            return None
        
        try:
            # Validate timeframe and convert to MT5 value
            tf = self._map_timeframe(timeframe)
            
            # Validate count
            count = max(N, 2)  # Ensure minimum of 2 bar
            
            # Fetch data from MT5
            from_pos = 1
            rates_data = mt5.copy_rates_from_pos(symbol, tf, from_pos, count)
            
            # Check if we got any data
            if rates_data is None or len(rates_data) == 0:
                logger.warning(f"No data returned for {symbol} @ {timeframe}")
                return pl.DataFrame()
            
            # Convert to Polars DataFrame
            bars = pl.DataFrame(rates_data)
            
            # Process the data
            return (
                bars
                .with_columns(
                    # mt5.time is seconds → ms → datetime
                    (pl.col("time") * 1_000).cast(pl.Datetime("ms")).alias("datetime")
                )
                .rename({
                    "tick_volume": "tickvol",
                    "real_volume": "volume"
                })
                .select([
                    "datetime", "open", "high", "low", "close", "tickvol", "volume", "spread"
                ])
            )
            
        except KeyError as e:
            logger.error(f"Missing expected column in MT5 data for {symbol}: {e}")
            return None
        except ValueError as e:
            logger.error(f"Invalid value in data processing for {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to get bars for {symbol} @ {timeframe}: {str(e)}")
            return None


    def get_latest_tick(self, symbol: str) -> dict:
        """
        Returns the latest tick data for a given symbol.

        Args:
            symbol (str): The symbol to retrieve the latest tick data for.
        
        Returns:
            dict: A dictionary containing the latest tick data for the given symbol. It will be empty if the symbol is not available.
        """
        try:
            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                logger.warning(f"[get_latest_tick] Symbol '{symbol}' is not available in the tick data set.")
                return {}

        except Exception as e:
            logger.error(f"Could not get tick for symbol '{symbol}'. Error: {e}")
            raise
        else:
            tick_dict = {
                'time': tick.time,
                'bid': Decimal(str(tick.bid)),
                'ask': Decimal(str(tick.ask)),
                'last': Decimal(str(tick.last)),
                'volume': tick.volume,
                'time_msc': tick.time_msc,
                'flags': tick.flags,
                'volume_real': Decimal(str(tick.volume_real)),
                }
            return tick_dict

    def get_latest_bid(self, symbol: str) -> Decimal:
        """
        Returns the latest bid price for a given symbol.

        Args:
            symbol (str): The symbol to retrieve the latest bid price for.

        Returns:
            Decimal: The latest bid price for the given symbol.
        """
        return self.get_latest_tick(symbol)['bid']
    
    def get_latest_ask(self, symbol: str) -> Decimal:
        """
        Returns the latest ask price for a given symbol.

        Args:
            symbol (str): The symbol to retrieve the latest ask price for.

        Returns:
            Decimal: The latest ask price for the given symbol.
        """
        return self.get_latest_tick(symbol)['ask']
    
    def get_latest_datetime(self, symbol: str, timeframe: str = '1min') -> datetime:
        """
        Returns the latest datetime for a given symbol and timeframe bar (not tick!).

        Args:
            symbol (str): The symbol to retrieve the latest datetime for.
            timeframe (str, optional): The timeframe to retrieve the latest datetime for. Defaults to '1min'.

        Returns:
            pd.Timestamp: The latest datetime for the given symbol and timeframe.
        """
        bar_event = self.get_latest_bar(symbol, timeframe)
        return bar_event.datetime #return latest_bar.name
    
    def update_bars(self) -> list:
        """
        Checks if there are new bars available for the symbols in the symbol list and if so, it generates a BarEvent.
        """
        events_container_list = []
        symbol_is_futures = False
        
        # New version to generate event Bar event for each new timefrme
        for symbol in self.symbol_list:
            # Need to check if the symbol is in the futures_tuple, if so, we need to get the correct contract to trade
            # if symbol in self.futures_tuple:  # symbol will always be a BASE futures symbol.
            #     symbol_is_futures = True
            #     # Here we will need to get the correct contract for the symbol, and rename the symbol: from ES to ES_U, for example
            #     symbol_contract = Utils.get_dzero_contract_to_trade(symbol)
                
            #     # We also need to check if this contract is already in the Market Watch, if not, we need to add it
            #     if mt5.symbol_info(symbol_contract) is None or not mt5.symbol_info(symbol_contract).visible:
            #         if not mt5.symbol_select(symbol_contract, True):
            #             logger.warning(f"Could not add {symbol_contract} to MarketWatch: {mt5.last_error()}")
            #             continue
            #         else:
            #             logger.info(f"DP: New Futures Contract {symbol_contract} has been successfully added to the MarketWatch!")
            
            for timeframe in self.timeframes_list:
                # Get the latest bar for the symbol and timeframe
                # if symbol_is_futures:
                #     latest_bar = self.get_latest_bar(symbol_contract, timeframe)
                # else:
                latest_bar = self.get_latest_bar(symbol, timeframe)

                # If the latest bar is None, we skip to the next symbol
                if latest_bar is None:
                    continue
                
                # If the latest bar is newer than the last bar seen for that timeframe, then we generate a BarEvent
                if latest_bar.datetime > self.last_bar_tf_datetime[symbol][timeframe]:
                    # Update the latest seen datetime
                    self.last_bar_tf_datetime[symbol][timeframe] = latest_bar.datetime

                    # Add the Bar Event to the event list.
                    # Reuse the bar we already fetched and gated on above instead of
                    # querying MT5 a second time: a re-fetch can straddle a timeframe
                    # boundary and return a different (newer) bar than the one recorded
                    # in last_bar_tf_datetime, which would skip the gated bar and
                    # duplicate the next one. It also avoids a redundant IPC round-trip.
                    # if symbol_is_futures:
                    #     events_container_list.append(self.get_latest_bar(symbol_contract, timeframe))
                    # else:
                    events_container_list.append(latest_bar)
        
        return events_container_list

