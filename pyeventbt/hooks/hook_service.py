"""
PyEventBT
Documentation: https://pyeventbt.com
GitHub: https://github.com/marticastany/pyeventbt

Author: Marti Castany
Copyright (c) 2025 Marti Castany
Licensed under the Apache License, Version 2.0
"""

from enum import Enum
from typing import Dict, Callable, List, Optional

from pyeventbt.strategy.core.modules import Modules
from pyeventbt.events.events import EventBase

class Hooks(str, Enum):
    ON_START = 'ON_START'
    """Hook that executes on the start of backtest or live run
    """
    ON_SIGNAL_EVENT = 'ON_SIGNAL_EVENT'
    """Hook that is executed when a SIGNAL_EVENT has being captured (When alpha model generates a signal event)
    """
    ON_ORDER_EVENT = 'ON_ORDER_EVENT'
    """Hook that is executed when a ORDER_EVENT has being captured (RISK_ENGINE approves SuggestedOrder)
    """
    ON_FILL_EVENT = 'ON_FILL_EVENT'
    """Hook that is executed when a FILL_EVENT has being captured (When a trade is executed)
    """
    ON_END = 'ON_END'
    """Hook that is executed when the end of the backtest/live trading has being reach
    """
    
    def __hash__(self) -> int:
        return hash(self.name)

class HookService:
    def __init__(self) -> None:
        self.__hooks_callbacks: Dict[Hooks, List[Callable[[Modules], None]]] = {}
        self.__hooks_enabled = True
    
    def enable_hooks(self):
        self.__hooks_enabled = True
    
    def disable_hooks(self):
        self.__hooks_enabled = False
    
    def add_hook(self, hook: Hooks, callback: Callable[[Modules], None]):
        self.__hooks_callbacks.setdefault(hook, []).append(callback)
        
    def call_callbacks(self, hook: Hooks, modules: Modules, event: Optional[EventBase] = None):
        if not self.__hooks_enabled:
            return
        
        for callback in self.__hooks_callbacks.get(hook, []):
            if event is not None:
                try:
                    # Try calling with both modules and event
                    callback(modules, event)
                except TypeError:
                    # Fallback for backward compatibility with legacy callbacks
                    callback(modules)
            else:
                callback(modules)