from src.unity.staging_to_curated.base_unity_table_class import UnityTableUtils

class StaticOverwriteUnityTable(UnityTableUtils):
    """
    Overwrite writing_mode for staging to curated class.
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def setup_staging_to_curated(self):
        """
        Static overwrite staging to curated. Overwrite existing partitions.
        """
        self.logger.info("Overwriting (partitionOverwriteMode static) unity table to curated ...")
        self.df = self.df.write.format("delta").mode("overwrite").option("partitionOverwriteMode", "static")
        return self

class DynamicOverwriteUnityTable(UnityTableUtils):
    """
    Append writing_mode for staging to curated class with mode overwrite.
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def setup_staging_to_curated(self):
        """
        Dynamic overwrite staging to curated. If partitions do not exist, append else static overwrite.
        """
        self.logger.info("Appending unity table to curated, overwriting partitions (partitionOverwriteMode dynamic) ...")
        self.df = self.df.write.format("delta").mode("overwrite").option("partitionOverwriteMode", "dynamic")
        return self
    
class AppendUnityTable(UnityTableUtils):
    """
    Append writing_mode for staging to curated class with mode append.
    """
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def setup_staging_to_curated(self):
        """
        Append staging to curated. Append new partitions, append data to existing partitions.
        """
        self.logger.info("Appending unity table to curated ...")
        self.df = self.df.write.format("delta").mode("append")
        return self
