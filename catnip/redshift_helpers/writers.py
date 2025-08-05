import pandas as pd
from pandera import DataFrameModel

import polars as pl
from pandera.polars import DataFrameModel as PolarsDataFrameModel

from catnip.redshift_helpers.validators import ColumnValidator
from catnip.redshift_helpers.lookups import METADATA_TABLE_NAME

from datetime import datetime
from typing import List, Dict, Any, Literal, TYPE_CHECKING
from io import BytesIO
from uuid import uuid4

if TYPE_CHECKING:
    from catnip.fla_redshift_v2 import FLA_Redshift_v2

class PandasWriter:

    # initialize
    def __init__(self, df: pd.DataFrame):
        self.df = df

    # chunk data into data structure -- list of filenames and parquet buffers
    def chunk_data(
        self,
        output_schema: DataFrameModel,
        chunk_size: int,
        processed_date: datetime
    ) -> List[Dict[str, str | BytesIO]]:

        ## Initialize data
        data = []

        ## Get total # of rows
        total_rows = self.get_num_rows()

        if output_schema:
            ## validate schema
            self.df = output_schema.validate(self.df)
            ## reorder columns
            self.reorder_columns(output_schema)

        ## Get Chunky
        for chunk_num, i in enumerate(range(0, total_rows, chunk_size), start=1):

            ## get chunk
            chunk = self.df.iloc[i:i+chunk_size]
            chunk = chunk.replace('', pd.NA)

            ## validate columns
            ColumnValidator(column_names=self.get_column_names()).validate_column_names()

            ## create buffer
            buffer = BytesIO()

            ## create processed date field
            chunk['processed_date'] = processed_date
            
            ## convert to parquet
            chunk.to_parquet(buffer, index=False, engine="pyarrow", coerce_timestamps="us", compression="zstd")
            buffer.seek(0)

            ## append to data structure
            data.append({
                "filename": f"chunk_{chunk_num:03d}.parquet",
                "data": buffer
            })
            
        return data

    # Returns the dtypes of the DataFrame columns
    def get_column_dtypes(self) -> List[str]:
        return [str(dtype.name).lower() for dtype in self.df.dtypes.tolist()]
    
    # return column names
    def get_column_names(self) -> List[str]:
        return list(self.df.columns)

    # Returns the number of rows in the LazyFrame
    def get_num_rows(self) -> int:
        return len(self.df.index)
    
    # Reorder columns in the DataFrame
    def reorder_columns(self, output_schema: DataFrameModel) -> None:
        self.df = self.df.reindex(columns = [x for x in [*output_schema.to_schema().columns] if x in self.df.columns.to_list()])
        return None


class PolarsWriter:

    # initialize
    def __init__(self, lf: pl.LazyFrame):
        self.lf = lf

    # chunk data into data structure -- list of filenames and parquet buffers
    def chunk_data(
        self,
        output_schema: PolarsDataFrameModel,
        chunk_size: int,
        processed_date: datetime
    ) -> List[Dict[str, str | BytesIO]]:

        ## Initialize data
        data = []

        ## Get total # of rows
        total_rows = self.get_num_rows()

        if output_schema:
            ## validate schema
            self.lf = output_schema.validate(self.lf)
            ## reorder columns
            self.reorder_columns(output_schema)

        ## Get Chunky
        for chunk_num, i in enumerate(range(0, total_rows, chunk_size), start=1):

            ## get chunk
            chunk = self.lf.slice(i, chunk_size)
            chunk = chunk.with_columns(pl.col(pl.Utf8).replace("", None))

            ## validate columns
            ColumnValidator(column_names=self.get_column_names()).validate_column_names()

            ## create buffer
            buffer = BytesIO()

            ## create processed date field
            chunk = chunk.with_columns(pl.lit(processed_date).alias("processed_date"))
            chunk = chunk.collect()
            
            ## convert to parquet
            chunk.write_parquet(buffer, compression="zstd")
            buffer.seek(0)

            ## append to data structure
            data.append({
                "filename": f"chunk_{chunk_num:03d}.parquet",
                "data": buffer
            })
            
        return data

    # Returns the dtypes of the LazyFrame columns
    def get_column_dtypes(self) -> List[str]:
        return [str(dtype).lower() for dtype in self.lf.collect_schema().dtypes()]
    
    # return column names
    def get_column_names(self) -> List[str]:
        return self.lf.collect_schema().names()
    
    # Returns the number of rows in the LazyFrame
    def get_num_rows(self) -> int:
        return self.lf.select(pl.count()).collect().item()

    # Reorder columns in the DataFrame
    def reorder_columns(self, output_schema: PolarsDataFrameModel) -> None:
        self.lf = self.lf.select([x for x in [*output_schema.to_schema().columns] if x in self.lf.columns])
        return None


class MetadataWriter:

    def __init__(self):
        pass

    def create_metadata_dictionary(
        self,
        operation: Literal["write", "append", "query", "query-unload", "delete", "create-table", "create-table-as", "copy"],
        table_name: str,
        table_schema: str,
        sql_query: str,
        processed_date: datetime,
        status: Literal["success", "failure"],
        error: str,
        execution_time: float,
        num_rows: int,
        s3_metadata: Dict[str, Any],
        s3_folder: str
    ) -> Dict[str, Any]:

        ## create metadata dictionary
        metadata = {
            "log_id": str(uuid4()),
            "operation": operation,
            "table_schema": table_schema,
            "table_name": table_name,
            "sql_query": sql_query and sql_query.replace("'", "''"),  # Escape single quotes for SQL
            "status": status,
            "is_successful": status == "success",
            "error": error and error.replace("'", "''"),
            "execution_time_seconds": execution_time,
            "num_rows": num_rows,
            "s3_num_files": s3_metadata and s3_metadata.get('num_files'),
            "s3_total_file_size_mb": s3_metadata and s3_metadata.get('total_file_size_mb'),
            "s3_folder": s3_folder,
            "processed_date": processed_date
        }

        return metadata
    
    def insert_metadata(self, metadata: Dict, redshift_client: "FLA_Redshift_v2") -> None:

        columns = ", ".join(f'"{key}"' for key in metadata.keys())
        values = ", ".join('NULL' if v is None or v is pd.NA else f"'{v}'" for v in metadata.values())

        q = f"""
            -- DROP TABLE IF EXISTS {METADATA_TABLE_NAME};

            CREATE TABLE IF NOT EXISTS {METADATA_TABLE_NAME} (

                -- Define the columns with appropriate data types and encodings
                log_id                  varchar(256) encode LZO,
                operation               varchar(256) encode LZO,
                table_schema            varchar(256) encode LZO,
                table_name              varchar(256) encode LZO,
                sql_query               varchar(65535) encode LZO,
                status                  varchar(256) encode LZO,
                is_successful           boolean encode RAW,
                error                   varchar(65535) encode LZO,
                execution_time_seconds  double precision encode RAW,
                num_rows                bigint encode AZ64,
                s3_folder               varchar(256) encode LZO,
                s3_num_files            bigint encode AZ64,
                s3_total_file_size_mb   double precision encode RAW,
                processed_date          timestamp encode AZ64
            );
        
            INSERT INTO {METADATA_TABLE_NAME}
                ({columns}) VALUES ({values});
        """

        redshift_client.execute_and_commit(q)

        return None