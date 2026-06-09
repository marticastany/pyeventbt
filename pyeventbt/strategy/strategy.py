"""
PyEventBT
Documentation: https://pyeventbt.com
GitHub: https://github.com/marticastany/pyeventbt

Author: Marti Castany
Copyright (c) 2025 Marti Castany
Licensed under the Apache License, Version 2.0
"""

import os
from pyeventbt.backtest.core.backtest_results import BacktestResults
from pyeventbt.broker.mt5_broker.shared.shared_data import SharedData
from pyeventbt.core.entities.hyper_parameter import HyperParameter
from pyeventbt.hooks.hook_service import HookService, Hooks

from pyeventbt.strategy.core.walk_forward import WalkForwardResults, WalkforwardType
from pyeventbt.trading_context.trading_context import TypeContext
from pyeventbt.trading_director.trading_director import TradingDirector

from pyeventbt.signal_engine.core.configurations.signal_engine_configurations import MACrossoverConfig
from pyeventbt.signal_engine.core.interfaces.signal_engine_interface import ISignalEngine

from pyeventbt.strategy.core.modules import Modules
from pyeventbt.strategy.core.strategy_timeframes import StrategyTimeframes

from pyeventbt.data_provider.core.configurations.data_provider_configurations import CSVBacktestDataConfig, MT5LiveDataConfig
from pyeventbt.data_provider.services.data_provider_service import DataProvider

from pyeventbt.events.events import BarEvent, ScheduledEvent, SignalEvent

from pyeventbt.execution_engine.core.configurations.execution_engine_configurations import MT5LiveExecutionConfig, MT5SimulatedExecutionConfig
from pyeventbt.execution_engine.services.execution_engine_service import ExecutionEngine

from pyeventbt.portfolio.portfolio import Portfolio
from pyeventbt.portfolio_handler.core.entities.suggested_order import SuggestedOrder

from pyeventbt.sizing_engine.core.configurations.sizing_engine_configurations import MinSizingConfig, RiskPctSizingConfig, FixedSizingConfig
from pyeventbt.risk_engine.core.configurations.risk_engine_configurations import PassthroughRiskConfig
from pyeventbt.trading_director.core.configurations.trading_session_configurations import MT5BacktestSessionConfig, MT5LiveSessionConfig

from pyeventbt.signal_engine.services.signal_engine_service import SignalEngineService
from pyeventbt.sizing_engine.services.sizing_engine_service import SizingEngineService

from pyeventbt.portfolio_handler.portfolio_handler import PortfolioHandler
from pyeventbt.risk_engine.services.risk_engine_service import RiskEngineService
#from pyeventbt.optimization.cost_functions import cagr_dd_ratio_cost_function

from queue import Queue
from typing import Callable
from datetime import datetime, timedelta
from functools import partial
from pyeventbt.utils.utils import LoggerColorFormatter
from pyeventbt.utils.utils import TerminalColors, colorize
from .core.account_currencies import AccountCurrencies
from .core.verbose_level import VerboseLevel
#import uuid
import pandas as pd
#from hyperopt import fmin, hp, tpe
#from hyperopt.exceptions import AllTrialsFailed
from pyeventbt.config import Mt5PlatformConfig
import logging

# Set up the logger
logger = logging.getLogger("pyeventbt")
logger.propagate = False

class Strategy:
    
    def __init__(self, logging_level: VerboseLevel = VerboseLevel.INFO) -> None:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(LoggerColorFormatter())
        logger.addHandler(console_handler)
        logger.setLevel(logging_level)
        logger.info("Setting up Strategy...")
        
        self.__initial_config()

    def __initial_config(self):
        ################## DEFINITION OF CORE FRAMEWORK OBJECTS ##################
        
        self.__sizing_engine_config = None
        self.__signal_engine_config = None
        self.__risk_engine_config = PassthroughRiskConfig()
        
        self.__signal_engines: dict[str, ISignalEngine] = {}
        self.__sizing_engines: dict[str, SizingEngineService] = {}
        self.__risk_engines: dict[str, RiskEngineService] = {}
        
        self.__strategy_id_mg_number_map: dict[str, int] = {} # mapping between strategy ids and magic numbers (for maximun compatibility)

        self.__strategy_timeframes = []

        self.__run_schedules = True
        self.__hooks = HookService()
        
        self.__scheduled_events: dict[StrategyTimeframes, list[Callable[[ScheduledEvent, Modules], None]]] = {}

    def hook(self, hook: Hooks):
        """Decorator to add hooks for executing function on desired hook

        >>> strategy = Strategy()
        >>> @strategy.hook(Hooks.ON_START)
        >>> def on_start_hook(modules: Modules) -> None:
        >>>    print("Hello from 'ON_START' hook :D")

        Args:
            hook (Hooks): The hook in which to execute the passed function
        """
        
        def decorator(fn: Callable[[Modules], None]):
            self.__hooks.add_hook(hook, fn)
        
        return decorator

    def enable_hooks(self):
        self.__hooks.enable_hooks()
    
    def disable_hooks(self):
        self.__hooks.disable_hooks()
        
    # this is the decorator that the user will use to declare their signal_engine
    def custom_signal_engine(self, strategy_id: str = 'default', strategy_timeframes: list[StrategyTimeframes] | None = None):
        """Decorator to set a custom signal engine for a strategy.
        The decorator must be used in the following way:

        >>> strategy = Strategy()
        >>> @strategy.custom_signal_engine()
        >>> def my_signal_engine(bar_event: BarEvent, modules: Modules) -> SignalEvent:
        >>>    ...
        >>>    return signal_event

        The function must have the following signature:

        my_signal_engine(bar_event: BarEvent, portfolio: Portfolio, execution_engine: IExecutionEngine, modules: Modules) -> SignalEvent

        The function must return a SignalEvent object.
        The function can also take any other arguments that are required by the signal engine.
        The function can also return None if no signal is generated.
        The function can also raise an exception if an error occurs.
        The function can also return a list of SignalEvent objects if multiple signals are generated.
        The function can also return a list of SignalEvent objects and a list of SuggestedOrder objects if multiple signals and suggested orders are generated.

        For configuring the strategy to run in before diffent timeframes (1min, 1h, 1d...) use the strategy_timeframes list
        
        >>> strategy = Strategy()
        >>> @strategy.custom_signal_engine(strategy_timeframes = [StrategyTimeframes.ONE_MIN, StrategyTimeframes.ONE_WEEK])
        >>> def my_signal_engine(bar_event: BarEvent, modules: Modules) -> SignalEvent:
        >>>    if event.timeframe == StrategyTimeframes.ONE_MIN:
        >>>         # Do wathever your strategy should do for every one min bar
        >>>    elif event.timeframe == StrategyTimeframes.ONE_WEEK:
        >>>         # Do what ever your strategy needs to do every week

        Args:
            strategy_id (str, optional): The id of the strategy this signal engine belongs to. Defaults to 'default'.
            strategy_timeframes (List[StrategyTimeframes]): The timesframes at which this strategy is going to be called .Defaults to StrategyTimeframes.ONE_MIN

        """
        if strategy_timeframes is None:
            strategy_timeframes = [StrategyTimeframes.ONE_MIN]

        for timeframe in strategy_timeframes:
            self.__strategy_timeframes.append(timeframe) if timeframe not in self.__strategy_timeframes else None

        def decorator(fn: Callable[[BarEvent, Modules], SignalEvent]):
            self.__signal_engines.setdefault(strategy_id, fn)
        
        return decorator

    def custom_sizing_engine(self, strategy_id: str = 'default'):
        """Sets a custom sizing engine for a strategy.
        The decorator must be used in the following way:

        >>> @app.custom_sizing_engine()
        >>> def my_sizing_engine(signal_event: SignalEvent, modules: Modules) -> SuggestedOrder:
        >>>    ...
        >>>    return suggested_order

        The function must have the following signature:

        my_sizing_engine(signal_event: SignalEvent, modules: Modules) -> SuggestedOrder

        The function must return a SuggestedOrder object.
        The function can also take any other arguments that are required by the sizing engine.
        The function can also return None if no suggested order is generated.
        The function can also raise an exception if an error occurs.
        The function can also return a list of SuggestedOrder objects if multiple suggested orders are generated.
        The function can also return a list of SuggestedOrder objects and a list of SignalEvent objects if multiple suggested orders and signal events are generated.

        The function can also take the following arguments:

        signal_event: The signal event generated by the signal engine.

        Args:
            strategy_id (str, optional): _description_. Defaults to 'default'.
        """
        
        def decorator( fn: Callable[[SignalEvent, Modules], SuggestedOrder]):
            self.__sizing_engines.setdefault(strategy_id, fn)
            
        return decorator
    
    def custom_risk_engine(self, strategy_id: str = 'default'):
        """ Sets a custom risk engine for a strategy.
        The decorator must be used in the following way:

        >>> @app.custom_risk_engine()
        >>> def my_risk_engine(suggested_order: SuggestedOrder, modules: Modules) -> float:
        >>>    ...
        >>>    return risk_pct

        The function must have the following signature:

        my_risk_engine(suggested_order: SuggestedOrder, modules: Modules) -> float

        The function must return a float value between 0 and 1.
        The function can also take any other arguments that are required by the risk engine.
        The function can also return None if no risk is generated.
        The function can also raise an exception if an error occurs.
        The function can also return a list of float values between 0 and 1 if multiple risks are generated.
        The function can also return a list of float values between 0 and 1 and a list of SuggestedOrder objects if multiple risks and suggested orders are generated.

        The function can also take the following arguments:

        suggested_order: The suggested order generated by the sizing engine.

        Args:

        Args:
            strategy_id (str, optional): _description_. Defaults to 'default'.
        """
        
        def decorator(fn: Callable[[SuggestedOrder, Modules], float]):
            self.__risk_engines.setdefault(strategy_id, fn)
            
        return decorator
    
    def configure_predefined_signal_engine(self, conf: MACrossoverConfig, strategy_timeframes: list[StrategyTimeframes] | None = None):
        if strategy_timeframes is None:
            strategy_timeframes = [StrategyTimeframes.ONE_MIN]

        for timeframe in strategy_timeframes:
            self.__strategy_timeframes.append(timeframe) if timeframe not in self.__strategy_timeframes else None
        
        self.__signal_engine_config = conf
    
    def configure_predefined_sizing_engine(self, conf: MinSizingConfig | RiskPctSizingConfig | FixedSizingConfig):
        self.__sizing_engine_config = conf
        
    def configure_predefined_risk_engine(self, conf: PassthroughRiskConfig):
        self.__risk_engine_config = conf

    def __get_signal_engine(self, strategy_id: str, modules: Modules) -> SignalEngineService:
        sigeng = SignalEngineService(self.EVENTS_QUEUE, modules, self.__signal_engine_config)

        if self.__signal_engines.get(strategy_id, None) is not None:
            sigeng.set_signal_engine(self.__signal_engines[strategy_id])

        return sigeng

    def __get_sizing_engine(self, strategy_id: str, modules: Modules) -> SizingEngineService:
        sizeng = SizingEngineService(self.EVENTS_QUEUE, modules, self.__sizing_engine_config)

        if self.__sizing_engines.get(strategy_id, None) is not None:
            sizeng.set_suggested_order_function(self.__sizing_engines[strategy_id])
            
        return sizeng

    def __get_risk_engine(self, strategy_id: str, modules: Modules) -> RiskEngineService:
        risken = RiskEngineService(self.EVENTS_QUEUE, self.__risk_engine_config, modules)
        
        if self.__risk_engines.get(strategy_id, None) is not None:
            risken.set_custom_asses_order(self.__risk_engines[strategy_id])
        
        return risken
    
    def __create_mg_for_strategy_id(self, strategy_id: str):
        max_mg = max(self.__strategy_id_mg_number_map.values()) if len(self.__strategy_id_mg_number_map.values()) != 0 else 0
        return self.__strategy_id_mg_number_map.setdefault(strategy_id, max_mg + 1)
    
    def run_every(self, interval: StrategyTimeframes):
        
        if interval not in self.__strategy_timeframes:
            self.__strategy_timeframes.append(interval)
        
        def decorator(fn: Callable[[ScheduledEvent, Modules], None]):
            self.__scheduled_events.setdefault(interval, []).append(fn)
            
        return decorator

    def deactivate_schedules(self):
        self.__run_schedules = False
        
    def activate_schedules(self):
        self.__run_schedules = True

    ############################# CREATION OF BACKTEST OBJECTS AND LAUNCHING THE SIMULATOR #############################
    def backtest(
            self,
            strategy_id: str = "123456",
            initial_capital: float = 10000.0,
            account_currency: AccountCurrencies = AccountCurrencies.USD,
            account_leverage: int = 30, 
            start_date: datetime = datetime(year=1970, month=1, day=1),
            end_date: datetime = datetime.now(),
            backtest_name: str = "Backtests",
            symbols_to_trade: list[str] | None = None,
            csv_dir: str|None = None,
            run_scheduled_taks: bool = False,
            export_backtest_csv: bool = False,
            export_backtest_parquet: bool = True,
            backtest_results_dir: str|None = None,
            data_provider_factory=None,
        ):
        if symbols_to_trade is None:
            symbols_to_trade = ['EURUSD']

        # Reset SharedData to YAML defaults so sequential backtests (e.g. optimization loops)
        # start with clean simulator state (symbol_info, account_info, etc.)
        SharedData()

        # the queue is instantited here to avoid problems when performing a backtest inside a backtest.
        self.EVENTS_QUEUE = Queue()

        # Set the trading context
        trading_context = TypeContext.BACKTEST
        
        # Set the data provider csv directory
        csv_dir = csv_dir if csv_dir is not None else os.path.join(os.path.dirname(os.path.abspath(__file__)),"..", "data_provider", "connectors", "historical_csv_data")
        
        
        # sort strategy timeframes in ascending order
        self.__strategy_timeframes.sort()
        
        # Set the data provider configuration and create the data provider object
        bt_data_provider_config = CSVBacktestDataConfig(
            csv_path=csv_dir,
            account_currency=account_currency,
            tradeable_symbol_list=symbols_to_trade,
            base_timeframe=self.__strategy_timeframes[0],
            timeframes_list=self.__strategy_timeframes[0:],
            backtest_start_timestamp= start_date,
            backtest_end_timestamp = end_date
        )
        
        DATA_PROVIDER = (
            data_provider_factory(self.EVENTS_QUEUE)
            if data_provider_factory is not None
            else DataProvider(self.EVENTS_QUEUE, bt_data_provider_config, trading_context)
        )
        
        # mgn = int(self.__create_mg_for_strategy_id(strategy_id))
        
        # Set the execution engine configuration and create the execution engine object
        execution_config = MT5SimulatedExecutionConfig(
            initial_balance=initial_capital,
            account_currency=account_currency,
            account_leverage=account_leverage,
            magic_number=int(strategy_id)
        )
        
        EXECUTION_ENGINE = ExecutionEngine(self.EVENTS_QUEUE, DATA_PROVIDER, execution_config)
        
        # Set the portfolio object
        PORTFOLIO = Portfolio(initial_balance=initial_capital,
                            execution_engine=EXECUTION_ENGINE,
                            trading_context=trading_context,
                            base_timeframe=self.__strategy_timeframes[0])
        
        # Set the modules object
        modules = Modules(
            TRADING_CONTEXT=trading_context,
            DATA_PROVIDER=DATA_PROVIDER,
            EXECUTION_ENGINE=EXECUTION_ENGINE,
            PORTFOLIO=PORTFOLIO
        )
        
        signal_engine = self.__get_signal_engine(strategy_id, modules)
        sizing_engine = self.__get_sizing_engine(strategy_id, modules)
        risk_engine = self.__get_risk_engine(strategy_id, modules)   
        
        # Set the trading session configuration
        TRADING_SESSION_CONFIG = MT5BacktestSessionConfig(
            start_date=start_date if start_date is not None else datetime(1970, 1, 1),
            initial_capital=initial_capital,
            backtest_name=backtest_name,
        )
        
        # Set the portfolio handler object
        PORTFOLIO_HANDLER = PortfolioHandler(
            events_queue=self.EVENTS_QUEUE,
            sizing_engine=sizing_engine,
            risk_engine=risk_engine,
            portfolio=PORTFOLIO,
            base_timeframe=self.__strategy_timeframes[0],
            backtest_results_dir=backtest_results_dir
        )
        
        TRADING_DIRECTOR = TradingDirector(
            events_queue=self.EVENTS_QUEUE,
            signal_engine_service=signal_engine,
            portfolio_handler=PORTFOLIO_HANDLER,
            trading_session_config=TRADING_SESSION_CONFIG,
            modules=modules,
            run_schedules=self.__run_schedules,
            export_backtest=export_backtest_csv,
            export_backtest_parquet=export_backtest_parquet,
            backtest_results_dir=backtest_results_dir,
            hook_service=self.__hooks
        )
        # FIXME -> At some point we must move everything on Trading Director to strategy        

        # add all passed schedules
        for timeframe, functions in self.__scheduled_events.items():
            for function in functions:
                TRADING_DIRECTOR.add_schedule(timeframe, function)

        # Capture start time
        start_time = datetime.now()
        
        # Run the backtest
        results: BacktestResults = TRADING_DIRECTOR.run()

        # Capture end time
        end_time = datetime.now()

        # Calculate duration
        duration = end_time - start_time
        hours, remainder = divmod(duration.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        logger.warning(f"Backtest performed in {hours} hours, {minutes} minutes and {seconds} seconds.")
        
        return results
    
    
    ############################# CREATION OF OBJECTS AND LAUNCH LIVE EXECUTION #############################
    def run_live(
        self,
        mt5_configuration: Mt5PlatformConfig,  
        strategy_id: str = "default",
        initial_capital: float = 10000.0,
        symbols_to_trade: list[str] | None = None,
        heartbeat: float = 0.1,
        ):
        if symbols_to_trade is None:
            symbols_to_trade = ['EURUSD']

        self.EVENTS_QUEUE = Queue()

        # Set the LIVE trading context
        trading_context = TypeContext.LIVE

        # Set the timeframe list and add the strategy timeframe to the list, which its first element is the base timeframe (one min for LIVE)
        self.__strategy_timeframes.sort()

        # Set the data provider configuration and create the data provider object
        data_config = MT5LiveDataConfig(
            tradeable_symbol_list=symbols_to_trade,
            timeframes_list=self.__strategy_timeframes,
            plaform_config=mt5_configuration
        )
        
        DATA_PROVIDER = DataProvider(self.EVENTS_QUEUE, data_config, trading_context)
        
        # Set the execution engine configuration and create the execution engine object
        execution_config = MT5LiveExecutionConfig(
            magic_number=int(strategy_id)
        )
        
        EXECUTION_ENGINE = ExecutionEngine(self.EVENTS_QUEUE, DATA_PROVIDER, execution_config)
        
        # Set the portfolio object
        PORTFOLIO = Portfolio(
            initial_balance=initial_capital,
            execution_engine=EXECUTION_ENGINE,
            trading_context=trading_context,
            base_timeframe=self.__strategy_timeframes[0]
        )
        
        # Set the modules object
        modules = Modules(
            TRADING_CONTEXT=trading_context,
            DATA_PROVIDER=DATA_PROVIDER,
            EXECUTION_ENGINE=EXECUTION_ENGINE,
            PORTFOLIO=PORTFOLIO
        )
        
        signal_engine = self.__get_signal_engine(strategy_id, modules)
        sizing_engine = self.__get_sizing_engine(strategy_id, modules)
        risk_engine = self.__get_risk_engine(strategy_id, modules)   
        
        # Set the trading session configuration
        TRADING_SESSION_CONFIG = MT5LiveSessionConfig(
            symbol_list=symbols_to_trade,
            heartbeat=heartbeat,
            platform_config=mt5_configuration
        )
        
        # Set the portfolio handler object
        PORTFOLIO_HANDLER = PortfolioHandler(
            events_queue=self.EVENTS_QUEUE,
            sizing_engine=sizing_engine,
            risk_engine=risk_engine,
            portfolio=PORTFOLIO,
            base_timeframe=self.__strategy_timeframes[0])
        
        # Set the trading director object
        TRADING_DIRECTOR = TradingDirector(
            events_queue=self.EVENTS_QUEUE,
            signal_engine_service=signal_engine,
            portfolio_handler=PORTFOLIO_HANDLER,
            trading_session_config=TRADING_SESSION_CONFIG,
            modules=modules,
            run_schedules=self.__run_schedules,
            hook_service=self.__hooks
        )

        # FIXME -> At some point we must move everithing on Trading Director to strategy        

        # add all passed schedules
        for timeframe, functions in self.__scheduled_events.items():
            for function in functions:
                TRADING_DIRECTOR.add_schedule(timeframe, function)

        # Run the live trading
        TRADING_DIRECTOR.run()