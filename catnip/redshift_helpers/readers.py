import pandas as pd
from pandera.typing import DataFrame
from pandera import DataFrameModel

import polars as pl
from pandera.typing.polars import DataFrame as PolarsDataFrame
from pandera.polars import DataFrameModel as PolarsDataFrameModel

from catnip.redshift_helpers.lookups import REDSHIFT_TYPE_MAPPING

from psycopg import Cursor

from typing import List
from botocore.response import StreamingBody
from io import BytesIO

class PandasReader:

    def __init__(self, input_schema: DataFrameModel = None):
        self.input_schema = input_schema

    def read_from_cursor(self, cursor: Cursor) -> pd.DataFrame:
        columns_list = [desc[0] for desc in cursor.description]
        if self.input_schema:
            return DataFrame[self.input_schema](cursor.fetchall(), columns=columns_list)
        else:
            return pd.DataFrame(cursor.fetchall(), columns=columns_list)
    
    def read_from_s3_stream(self, responses: List[StreamingBody]) -> pd.DataFrame:
        ## Take in list of StreamingBody responses and read them into a pandas DataFrame
        if self.input_schema:
            df_list = [DataFrame[self.input_schema](pd.read_parquet(BytesIO(response.read()))) for response in responses]
        else:
            df_list = [pd.read_parquet(BytesIO(response.read())) for response in responses]
        
        return pd.concat(df_list, ignore_index=True)

    # Returns the number of rows in the DataFrame
    def get_num_rows(self, df: pd.DataFrame) -> int:
        return len(df.index)


class PolarsReader:

    def __init__(self, input_schema: PolarsDataFrameModel = None):
        self.input_schema = input_schema

    def read_from_cursor(self, cursor: Cursor) -> pl.LazyFrame:
        columns_list = [desc[0] for desc in cursor.description]
        data_type_list = [REDSHIFT_TYPE_MAPPING[desc[1]] for desc in cursor.description]
        polars_schema = dict(zip(columns_list, data_type_list))

        if self.input_schema:
            return PolarsDataFrame[self.input_schema](cursor.fetchall(), schema=polars_schema).lazy()
        else:
            return pl.LazyFrame(cursor.fetchall(), schema=polars_schema, orient="row")

    def read_from_s3_stream(self, responses: List[StreamingBody]) -> pl.LazyFrame:
        ## Take in list of StreamingBody responses and read them into a polars LazyFrame
        if self.input_schema:
            df_list = [PolarsDataFrame[self.input_schema](pl.scan_parquet(BytesIO(response.read()))) for response in responses]
        else:
            df_list = [pl.scan_parquet(BytesIO(response.read())) for response in responses]
        
        return pl.concat(df_list)

    # Returns the number of rows in the LazyFrame
    def get_num_rows(self, lf: pl.LazyFrame) -> int:
        return lf.select(pl.len()).collect().item()