import logging
from abc import ABC, abstractmethod
from typing import Optional
from datetime import datetime
from src.unity.staging_to_curated.base_unity_table_class import UnityTableUtils, ExecutionStatus
from src.unity.utils.helpers import get_logger

class Observer(ABC):
    """
    Observer interface, declares update method for writing methods.
    """
    def __init__(self):
        self.logger: logging.Logger = get_logger()
        
    @abstractmethod
    def update(self, subject: UnityTableUtils) -> None:
        """
        Updates for unity table writing from staging to curated.
        """
        pass

class TimeObserver(Observer):
    """
    Time needed for completion of writing staging unity table to curated.
    """
    def __init__(self):
        super().__init__()
        self.start: Optional[datetime] = None
        self.end: Optional[datetime] = None
        self.total: Optional[datetime] = None
    
    def update(self, subject: UnityTableUtils) -> None:
        """
        Change of states of execution (write table staging to curated) accross time.
        """
        if subject.execution_status == ExecutionStatus.STARTED:
            self.start = datetime.now()
            self.logger.info(f"Execution started at: {self.start}")
        elif subject.execution_status == ExecutionStatus.COMPLETED:
            self.end = datetime.now()
            self.total = (self.end - self.start)
            self.logger.info(f"Execution ended at: {self.end}")
            self.logger.info(f"Total execution time: {self.total}")
