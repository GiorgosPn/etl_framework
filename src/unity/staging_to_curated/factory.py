from src.unity.staging_to_curated.setup_unity_table_utils import StaticOverwriteUnityTable, DynamicOverwriteUnityTable, AppendUnityTable
from src.unity.staging_to_curated.base_unity_table_class import UnityTableUtils
from enum import Enum

class WriteMode(Enum):
    """
    Enum for writing mode (overwrite, append)
    """
    OVERWRITE = "overwrite"
    APPEND = "append"
    
class UnityWriteModeFactory:
    """
    A factory that contains data write methods for Unity Tables.
    """

    @classmethod
    def get_write_mode(cls, pipeline: str, writing_mode: str) -> UnityTableUtils:
        """
        Method that returns the proper class for
        """
        if WriteMode.OVERWRITE.value in pipeline:
            return StaticOverwriteUnityTable
        elif WriteMode.APPEND.value in pipeline and writing_mode == WriteMode.OVERWRITE.value:
            return DynamicOverwriteUnityTable
        elif WriteMode.APPEND.value in pipeline and writing_mode == WriteMode.APPEND.value:
            return AppendUnityTable
        else:
            raise NotImplementedError("Selected writing mode not yet implemented!")
