"""
PyEventBT
Documentation: https://pyeventbt.com
GitHub: https://github.com/marticastany/pyeventbt

Author: Marti Castany
Copyright (c) 2025 Marti Castany
Licensed under the Apache License, Version 2.0
"""

from pyeventbt.signal_engine.services.signal_engine_service import SignalEngineService
from pyeventbt.hooks.hook_service import HookService, Hooks
from pyeventbt.schedule_service.schedule_service import ScheduleService
from pyeventbt.strategy.core.modules import Modules
from pyeventbt.events.events import BarEvent, ScheduledEvent, SignalEvent, OrderEvent, FillEvent, EventType
from pyeventbt.portfolio_handler.portfolio_handler import PortfolioHandler
from pyeventbt.broker.mt5_broker.connectors.live_mt5_broker import LiveMT5Broker
from pyeventbt.strategy.core.strategy_timeframes import StrategyTimeframes
from .core.configurations.trading_session_configurations import BaseTradingSessionConfig, MT5BacktestSessionConfig, MT5LiveSessionConfig
from pyeventbt.utils.utils import Utils
import pyeventbt.trading_context.trading_context as trading_context
from typing import Callable

import queue
import time
import logging

logger = logging.getLogger("pyeventbt")

class TradingDirector():
    
    def __init__(
                self, events_queue: queue.Queue, 
                signal_engine_service: SignalEngineService,
                portfolio_handler: PortfolioHandler,
                trading_session_config: BaseTradingSessionConfig,
                modules: Modules,
                run_schedules: bool = False,
                export_backtest: bool = False,
                export_backtest_parquet: bool = False,
                backtest_results_dir: str = None,
                hook_service: HookService = HookService()
            ) -> None:
        
        self.events_queue = events_queue
    
        # Definition of the event handlers dict
        self.event_handlers_dict = {
            EventType.BAR: self._handle_bar_event,
            EventType.SIGNAL: self._handle_signal_event,
            EventType.ORDER: self._handle_order_event,
            EventType.FILL: self._handle_fill_event
            }
        
        # Define the global instance variables that will be used in the trading director
        self.is_live_trading = False
        
        self._export_backtest_csv = export_backtest
        self._export_backtest_parquet = export_backtest_parquet
        self._backtest_results_dir = backtest_results_dir

        self.MODULES = modules

        # Global Tradoing Objects
        self.DATA_PROVIDER = modules.DATA_PROVIDER
        self.SIGNAL_GENERATOR = signal_engine_service
        self.EXECUTION_ENGINE = modules.EXECUTION_ENGINE
        self.PORTFOLIO_HANDLER = portfolio_handler
        self.SCHEDULE_SERVICE = ScheduleService(modules)
        self.HOOK_SERVICE = hook_service

        # Execute a method that configures the type of trading session (backtest or live)
        self._configure_session(trading_session_config)

        self.__run_schedules = run_schedules

    # interessant posar una flag que digui si el environment global es backtest o live
    def _configure_session(self, trading_session_config: BaseTradingSessionConfig) -> None:
        if isinstance(trading_session_config, MT5BacktestSessionConfig):
            self._configure_mt5_backtest_session(configuration=trading_session_config)
        
        elif isinstance(trading_session_config, MT5LiveSessionConfig):
            self._configure_mt5_live_session(configuration=trading_session_config)
    
    def _configure_mt5_backtest_session(self, configuration: MT5BacktestSessionConfig) -> None:
        self.initial_capital = configuration.initial_capital
        self.start_date = configuration.start_date
        self.backtest_name = configuration.backtest_name
        
    def _configure_mt5_live_session(self, configuration: MT5LiveSessionConfig) -> None:
        self.heartbeat = configuration.heartbeat
        self.is_live_trading = True

        # Now we need to establish a connection to the MT5 Platform
        self.LIVE_MT5_BROKER = LiveMT5Broker(configuration.symbol_list, config=configuration.platform_config)
    
    # EVENT HANDLERS
    def _handle_bar_event(self, event: BarEvent) -> None:
        self.PORTFOLIO_HANDLER.process_bar_event(event)  # Updates portfolio values
        self.SCHEDULE_SERVICE.run_scheduled_callbacks(event)
        self.SIGNAL_GENERATOR.generate_signal(event)

    def _handle_signal_event(self, event: SignalEvent) -> None:
        self.HOOK_SERVICE.call_callbacks(Hooks.ON_SIGNAL_EVENT, self.MODULES, event)
        self.PORTFOLIO_HANDLER.process_signal_event(event)

    def _handle_order_event(self, event: OrderEvent) -> None:
        self.EXECUTION_ENGINE._process_order_event(event)
        self.HOOK_SERVICE.call_callbacks(Hooks.ON_ORDER_EVENT, self.MODULES, event)

    def _handle_fill_event(self, event: FillEvent) -> None:
        self.HOOK_SERVICE.call_callbacks(Hooks.ON_FILL_EVENT, self.MODULES, event)
        self.PORTFOLIO_HANDLER.process_fill_event(event)

    def _handle_none_event(self, event):
        logger.warning(f"Received a NONE event: {event}")

    def _handle_backtest_end(self):
        logger.info(f"\x1b[95;20mBacktest ended")
        return self.PORTFOLIO_HANDLER.process_backtest_end(self.backtest_name, self._export_backtest_csv, self._export_backtest_parquet)

    def add_schedule(self, timeframe: StrategyTimeframes, fn: Callable[[ScheduledEvent, Modules], None] ):
        self.SCHEDULE_SERVICE.add_schedule(timeframe=timeframe, callback=fn)

    def run(self) -> None:
        """
        This is the main loop of the trading director. It will be called by the trading session.
        """
        self.HOOK_SERVICE.call_callbacks(Hooks.ON_START, self.MODULES)
        if self.is_live_trading:
            res = self._run_live_trading()
        else:
            res = self._run_backtest()
        self.HOOK_SERVICE.call_callbacks(Hooks.ON_END, self.MODULES)
        return res
    
    def _run_backtest(self) -> None:
        """
        This is the main loop of the trading director. It will be called by the trading session.
        """
        
        if not self.__run_schedules:
            self.SCHEDULE_SERVICE.deactivate_schedules()
        
        while self.DATA_PROVIDER.continue_backtest:
            try:
                event = self.events_queue.get(block=False)
            
            except queue.Empty:
                self.DATA_PROVIDER.update_bars()
            
            else:
                if event is not None:
                    # Execute the event handler method from the dict value
                    self.event_handlers_dict[event.type](event)
                else:
                    self._handle_none_event(event)
            
            if self.DATA_PROVIDER.close_positions_end_of_data:
                # If there are open positions, close them
                if self.EXECUTION_ENGINE._get_strategy_positions():
                    logger.info(f"Closing all positions at the end of the backtest")
                    self.EXECUTION_ENGINE.close_all_strategy_positions()
                    continue #Send control back to the loop to process fill events from the close positions at the end of the backtest
                
                # If there are no open positions, we need to process the end of the backtest
                if self.events_queue.empty():
                    self.DATA_PROVIDER.continue_backtest = False
            
        # Once the backtest ends, we need to process the end of the backtest
        return self._handle_backtest_end()
    
    def _run_live_trading(self) -> None:
        """
        This is the main loop of the trading director. It will be called by the trading session.
        """
        
        if not self.__run_schedules:
            self.SCHEDULE_SERVICE.deactivate_schedules()
        
        while True:
            try:
                event = self.events_queue.get(block=False)
            
            except queue.Empty:
                self.DATA_PROVIDER.update_bars()
            
            else:
                if event is not None:
                    # Execute the event handler method from the dict value
                    self.event_handlers_dict[event.type](event)
                else:
                    self._handle_none_event(event)
            
            time.sleep(self.heartbeat)