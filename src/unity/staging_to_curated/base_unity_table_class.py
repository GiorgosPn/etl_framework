import logging
from enum import Enum
from pyspark.sql import SparkSession
from typing import Optional, List
from abc import ABC, abstractmethod

class ExecutionStatus(Enum):
    """
    Writing table from staging to curated status of execution (enumeration).
    """
    NOT_STARTED = 0
    STARTED = 1
    COMPLETED = 2

class UnityTableUtils(ABC):
    """
    Base class, functionality for Unity Tables.
    """

    def __init__(
        self,
        source_system: str,
        staging_catalog_name: str,
        staging_schema_name: str,
        curated_catalog_name: str,
        curated_schema_name: str,
        table_name: str,
        in_production: bool,
        partition_cols: Optional[List[str]]=None
        ):
        
        # Source system, staging catalog & schema names, curated catalog & schema names, table name, partition columns
        self.source_system = source_system
        self.staging_catalog_name = staging_catalog_name
        self.staging_schema_name = staging_schema_name
        self.curated_catalog_name = curated_catalog_name
        self.curated_schema_name = curated_schema_name
        self.table_name = table_name
        self.spark = SparkSession.builder.getOrCreate()
        self.df = self.spark.read.table(f"{self.staging_catalog_name}.{self.staging_schema_name}.{self.table_name}")
        self.partition_cols = partition_cols
        # In production or not
        self.in_production = in_production
        # Execution status (writing staging table to curated.)
        self.execution_status = ExecutionStatus.NOT_STARTED
        # Logger
        self.logger: logging.Logger = get_logger()
        # Time observers
        from src.unity.observers.observers import Observer, TimeObserver
        self._observers: List[Observer] = []
        self._observers.append(TimeObserver())


    @abstractmethod
    def setup_staging_to_curated(self):
        pass

    def notify(self) -> None:
        """
        Trigger an update to each subscriber. 
        """
        for observer in self._observers:
            observer.update(self)

    def set_execution_status(self, execution_status: ExecutionStatus) -> None:
        """
        Set execution status when writing table from staging to curated.
        """
        self.execution_status = execution_status

    def partition_by(self):
         """
         Partition unity table using partition columns (if exist)
         Enhances dataframe write with spark partitionBy.
         """
         self.logger.info(f"Unity table will be partitioned by: {self.partition_cols}")
         self.df = self.df.partitionBy(self.partition_cols)
         return self

    def write(self):
        """
        Write table from staging to curated using partioning if needed.
        """

        # Set execution status : START
        self.set_execution_status(ExecutionStatus.STARTED)
        self.notify()
 
        try:
            # Partition table if needed and write table to curated
            curated_table = f"{self.curated_catalog_name}.{self.curated_schema_name}.{self.table_name}"
            # Enhance df with partitionBy, if partition columns exist
            if self.partition_cols:
                self.partition_by()
            # Write table to curated
            self.df.saveAsTable(curated_table)
            # Set executions status: COMPLETE and update observers
            self.set_execution_status(ExecutionStatus.COMPLETED)
            self.notify()
        except Exception as e:
            self.logger.error(f"Writing from staging to curated failed with error: {e}")
            raise e
