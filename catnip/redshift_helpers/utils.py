from catnip.redshift_helpers.writers import PandasWriter, PolarsWriter
from typing import List

class RedshiftTypeMapper:

    def __init__(self, writer: PandasWriter | PolarsWriter):
        self.writer = writer
    
    def map_types(self, varchar_max_list: List[str]) -> List[str]:
        """
        Maps the DataFrame column dtypes to Redshift dtypes.
        """
        column_data_types = [self._get_redshift_dtype(dtype) for dtype in self.writer.get_column_dtypes()]
        max_indexes = [idx for idx, val in enumerate(self.writer.get_column_names()) if val in varchar_max_list]
        column_data_types = ["VARCHAR(MAX)" if idx in max_indexes else val for idx, val in enumerate(column_data_types)]
        column_data_types.append("TIMESTAMP") # add processed_date column

        return column_data_types
    
    def map_encodings(self, column_data_types: List[str]) -> List[str]:
        """ 
        Maps the DataFrame column dtypes to Redshift encodings.
        """
        encoding_dict = {
            "INT8": "AZ64", 
            "INT": "AZ64", 
            "FLOAT8": "RAW", 
            "TIMESTAMP": "AZ64", 
            "TIMESTAMPTZ": "AZ64", 
            "BOOL": "RAW",
            "VARCHAR": "LZO"
        }
        
        return [encoding_dict[dt.split("(")[0]] for dt in column_data_types]
    
    def _get_redshift_dtype(self, dtype: str) -> str:

        if dtype.startswith("int64"):
            return "INT8"
        
        elif dtype.startswith("int"):
            return "INT"
        
        elif dtype.startswith("float"):
            return "FLOAT8"
        
        elif dtype.startswith("datetime"):
            if len(dtype.split(",")) > 1:
                return "TIMESTAMPTZ"
            else:
                return "TIMESTAMP"
        
        elif dtype in ["bool", "boolean"]:
            return "BOOL"
        
        else:
            return "VARCHAR(256)"