from pydantic import BaseModel, SecretStr
from typing import List, Dict, Literal

import pandas as pd
from pandera import DataFrameModel
# from pandera.typing import DataFrame

import polars as pl
from pandera.polars import DataFrameModel as PolarsDataFrameModel
# from pandera.typing.polars import DataFrame as PolarsDataFrame

from catnip.fla_s3 import FLA_S3
from catnip.redshift_helpers.writers import (
    PandasWriter, 
    PolarsWriter, 
    MetadataWriter
)
from catnip.redshift_helpers.readers import (
    PandasReader, 
    PolarsReader
)
from catnip.redshift_helpers.utils import RedshiftTypeMapper

import psycopg
from psycopg import Connection

import traceback
import sys

import time
from datetime import datetime
import pytz

import re
from uuid import uuid4

class FLA_Redshift(BaseModel):

    ## Database Info
    dbname: SecretStr
    host: SecretStr
    port: int
    user: SecretStr
    password: SecretStr

    ## S3 Bucket Info
    aws_access_key_id: SecretStr
    aws_secret_access_key: SecretStr
    bucket: SecretStr
    max_pool_connections: int = 10

    ## Options
    verbose: bool = True

    ## Import Pandera Schema
    input_schema: DataFrameModel | PolarsDataFrameModel = None
    output_schema: DataFrameModel | PolarsDataFrameModel = None

    @property
    def _s3(self):

        return FLA_S3(
            aws_access_key_id=self.aws_access_key_id.get_secret_value(),
            aws_secret_access_key=self.aws_secret_access_key.get_secret_value(),
            bucket=self.bucket.get_secret_value(),
            max_pool_connections=self.max_pool_connections
        )

    ######################
    ### USER FUNCTIONS ###
    ######################

    def write_to_warehouse(
        self,
        df: pd.DataFrame | pl.LazyFrame,
        table_name: str,
        table_schema: str = "custom",
        to_append: bool = False,
        chunk_size: int = 500000,
        varchar_max_list: List = [],
        diststyle: str = "even",
        distkey: str = "",
        sortkey: str = ""
    ) -> Dict:
        
        ## Start clock for execution time tracking
        start_time = time.perf_counter()

        ## Initialize
        unix_timestamp = int(time.time())
        processed_date = self._get_processed_date() 
        folder_name = f"{unix_timestamp}_{table_name}"

        ## Create writer
        if isinstance(df, pl.LazyFrame):
            writer = PolarsWriter(lf=df)
        elif isinstance(df, pd.DataFrame):
            writer = PandasWriter(df=df)
        else:
            raise TypeError("Dataframe passed in is neither a Pandas DataFrame, nor a Polars LazyFrame. Dummy.")
        
        ## Create parquet file buffers
        data = writer.chunk_data(
            output_schema=self.output_schema,
            chunk_size=chunk_size,
            processed_date=processed_date
        )

        ## Write to S3
        self._s3.upload_files(
            folder=folder_name,
            file_payloads=data
        )

        ## Set redshift table name
        redshift_table_name = f"{table_schema}.{table_name}"

        ## Create Redshift table if not appending
        if not to_append:
            self._create_redshift_table(
                writer=writer,
                redshift_table_name=redshift_table_name,
                varchar_max_list=varchar_max_list,
                diststyle=diststyle,
                distkey=distkey,
                sortkey=sortkey
            )

        ## Load data from S3 to Redshift
        try:
            self._s3_to_redshift(
                redshift_table_name=redshift_table_name,
                folder_name=folder_name
            )
            status = "success"
            error = None
        except Exception as e:
            print(f"ERROR: {e}")
            error = str(e)
            status = "failure"

        ## Create metadata dictionary and insert into metadata table
        metadata = MetadataWriter().create_metadata_dictionary(
            operation="write" if not to_append else "append",
            table_name=table_name,
            table_schema=table_schema,
            sql_query=None,
            processed_date=processed_date,
            status=status,
            error=error,
            execution_time=time.perf_counter() - start_time,
            num_rows=writer.get_num_rows(),
            s3_metadata=self._s3.get_folder_metadata(folder=folder_name),
            s3_folder=folder_name
        )

        MetadataWriter().insert_metadata(
            metadata=metadata,
            redshift_client=self
        )

        ## Raise exception if failed to write to warehouse
        if status == "failure":
            raise Exception(f"Failed to write to Redshift table {redshift_table_name}. Error: {error}")
        
        ## Return Metadata
        return metadata

    def query_warehouse(
        self,
        sql_string: str,
        return_polars: bool = False,
        use_unload: bool = False
    ) -> pd.DataFrame | pl.LazyFrame:

        ## Start clock for execution time tracking
        start_time = time.perf_counter()

        if return_polars:
            reader = PolarsReader()
        else:
            reader = PandasReader()
        
        if not use_unload:

            # connect
            connection = self._connect_to_redshift()
            cursor = connection.cursor()
            
            try:
                # execute query
                if self.verbose:
                    print(f"Executing query: {sql_string}")
                cursor.execute(sql_string)
                
                # fetch results
                df = reader.read_from_cursor(cursor)

                # close up shop
                cursor.close()
                connection.close()

                status = "success"
                error = None

            except Exception as e:
                print(f"ERROR: {e}")
                error = str(e)
                status = "failure"

            ## Create metadata dictionary and insert into metadata table
            metadata = MetadataWriter().create_metadata_dictionary(
                operation="query",
                table_name=None,
                table_schema=None,
                sql_query=sql_string,
                processed_date=self._get_processed_date(),
                status=status,
                error=error,
                execution_time=time.perf_counter() - start_time,
                num_rows=reader.get_num_rows(df) if error is None else None,
                s3_metadata=None,
                s3_folder=None
            )

            MetadataWriter().insert_metadata(
                metadata=metadata,
                redshift_client=self
            )

            ## Raise exception if failed to query warehouse
            if status == "failure":
                raise Exception(f"Failed to return results for {sql_string}. Error: {error}")
            
            return df
        
        else:

            # initialize
            folder = f"query_{int(time.time())}_{uuid4()}"

            try:
                # unload to s3
                self._unload_to_s3(
                    sql_string=sql_string,
                    folder_name=folder
                )

                # download from s3
                files = list(set(self._s3.get_all_filenames_in_folder(folder)))
                print(f"Downloading {len(files)} files from S3 folder '{folder}'")
                print(f"Files: {files}")

                responses = self._s3.download_files(folder=folder, filenames=files)

                # read into dataframe or lazyframe
                df = reader.read_from_s3_stream([v for v in responses.values()])

                status = "success"
                error = None

            except Exception as e:
                print(f"ERROR: {e}")
                error = str(e)
                status = "failure"
            
            ## Create metadata dictionary and insert into metadata table
            metadata = MetadataWriter().create_metadata_dictionary(
                operation="query-unload",
                table_name=None,
                table_schema=None,
                sql_query=sql_string,
                processed_date=self._get_processed_date(),
                status=status,
                error=error,
                execution_time=time.perf_counter() - start_time,
                num_rows=reader.get_num_rows(df) if error is None else None,
                s3_metadata=self._s3.get_folder_metadata(folder=folder),
                s3_folder=folder
            )

            MetadataWriter().insert_metadata(
                metadata=metadata,
                redshift_client=self
            )

            ## Raise exception if failed to query warehouse
            if status == "failure":
                raise Exception(f"Failed to return results for {sql_string}. Error: {error}")
            
            return df

    
    def execute_and_commit(
        self, 
        sql_string: str,
        insert_metadata: bool = False,
        query_type: Literal["delete", "create-table", "create-table-as", "drop"] = None
    ) -> None:

        ## Start clock for execution time tracking
        start_time = time.perf_counter()

        try:
            # connect
            connection = self._connect_to_redshift()
            
            # set cursor
            cursor = connection.cursor()
            connection.autocommit = True
            
            # execute and commit
            cursor.execute(sql_string)

            # close up shop
            cursor.close()
            connection.close()

            status = "success"
            error = None

        except Exception as e:
            print(f"ERROR: {e}")
            error = str(e)
            status = "failure"

        if insert_metadata:
            if query_type is None:
                raise ValueError("Must add a query type if you're inserting metadata, doofus!")
            ## Create metadata dictionary and insert into metadata table
            metadata = MetadataWriter().create_metadata_dictionary(
                operation=query_type,
                table_name=None,
                table_schema=None,
                sql_query=sql_string,
                processed_date=self._get_processed_date(),
                status=status,
                error=error,
                execution_time=time.perf_counter() - start_time,
                num_rows=None,
                s3_metadata=None,
                s3_folder=None
            )

            MetadataWriter().insert_metadata(
                metadata=metadata,
                redshift_client=self
            )

        ## Raise exception if failed to query warehouse
        if status == "failure":
            raise Exception(f"Failed to execute for {sql_string}. Error: {error}")
    
        return metadata if insert_metadata else None
    
    def copy_to_warehouse(
        self, 
        s3_folder: str,
        table_name: str,
        table_schema: str = "custom",
        is_parquet: bool = True,
        insert_metadata: bool = False
    ) -> None:

        ## Start clock for execution time tracking
        start_time = time.perf_counter()

        if not is_parquet:
            raise ValueError("Non-parquet functionality not currently supported. Sorry bub!")
        try:
            self._s3_to_redshift(
                redshift_table_name=f"{table_schema}.{table_name}",
                folder_name=s3_folder
            )
            status = "success"
            error = None

        except Exception as e:
            print(f"ERROR: {e}")
            error = str(e)
            status = "failure"

        if insert_metadata:
            ## Create metadata dictionary and insert into metadata table
            metadata = MetadataWriter().create_metadata_dictionary(
                operation="copy",
                table_name=table_name,
                table_schema=table_schema,
                sql_query=None,
                processed_date=self._get_processed_date(),
                status=status,
                error=error,
                execution_time=time.perf_counter() - start_time,
                num_rows=None,
                s3_metadata=self._s3.get_folder_metadata(folder=s3_folder),
                s3_folder=s3_folder
            )

            MetadataWriter().insert_metadata(
                metadata=metadata,
                redshift_client=self
            )

        ## Raise exception if failed to query warehouse
        if status == "failure":
            raise Exception(f"Failed to copy {s3_folder} into Redshift. Error: {error}")
    
        return metadata if insert_metadata else None
    
    ########################
    ### HELPER FUNCTIONS ###
    ########################

    def _connect_to_redshift(self) -> Connection:

        conn = psycopg.connect(
            dbname = self.dbname.get_secret_value(),
            host = self.host.get_secret_value(),
            port = self.port,
            user = self.user.get_secret_value(),
            password = self.password.get_secret_value()
        )

        return conn 

    def _get_processed_date(self):

        eastern = pytz.timezone('America/New_York')
        utc_now = datetime.now(pytz.utc)

        return utc_now.astimezone(eastern)
        
    def _create_redshift_table(
        self,
        writer: PandasWriter | PolarsWriter,
        redshift_table_name: str,
        varchar_max_list: List,
        diststyle: str,
        distkey: str,
        sortkey: str,
    ) -> None:

        ## get column names, data types, and encoded values to create table
        if self.output_schema:
            writer.reorder_columns(output_schema=self.output_schema)
        column_names = writer.get_column_names() + ["processed_date"]
        column_data_types = RedshiftTypeMapper(writer=writer).map_types(varchar_max_list=varchar_max_list)
        encoded_values = RedshiftTypeMapper(writer=writer).map_encodings(column_data_types=column_data_types)

        ## Create table query
        create_table_query = f""" 
            
            CREATE TABLE {redshift_table_name}
                ({
                    ", ".join([f"{c} {dt} ENCODE {e}" for c, dt, e in zip(column_names, column_data_types, encoded_values)])
                })

        """

        if not distkey:
            if diststyle not in ["even", "all"]:
                raise ValueError("dist_style must be either 'even' or 'all'! C'mon!")
            else:
                create_table_query += f" diststyle {diststyle}"
        else:
            create_table_query += f" distkey({distkey})"
        

        if sortkey:
            create_table_query += f" sortkey({sortkey})"

        ## Execute & commit
        if self.verbose:
            print(create_table_query)
            print("Creating a table in Redshift! 🤞")

        self.execute_and_commit(sql_string=f"DROP TABLE IF EXISTS {redshift_table_name};")
        self.execute_and_commit(sql_string=create_table_query)

        return None
    
    def _s3_to_redshift(
        self,
        redshift_table_name: str,
        folder_name: str,
        is_parquet: bool = True
    ) -> None:
        
        ## Construct query
        bucket_file_name = f"s3://{self.bucket.get_secret_value()}/{folder_name}"
        authorization_string = f"""
            access_key_id '{self.aws_access_key_id.get_secret_value()}'
            secret_access_key '{self.aws_secret_access_key.get_secret_value()}'
        """

        s3_to_sql = f"""
            COPY {redshift_table_name}
            FROM '{bucket_file_name}'
            {" FORMAT AS PARQUET" if is_parquet else ""}
            {authorization_string}
            ;
        """

        ## Execute & commit
        if self.verbose:
            # add logger!
            def mask_credentials(s: str) -> str:
                s = re.sub('(?<=access_key_id \')(.*)(?=\')', '*'*8, s)
                s = re.sub('(?<=secret_access_key \')(.*)(?=\')', '*'*8, s)
                return s 
            
            masked_credential_string = mask_credentials(s3_to_sql)
            print(f"{masked_credential_string}")
            print("Filling the table into Redshift! 🤞")


        with self._connect_to_redshift() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(s3_to_sql)
                    conn.commit()
                except Exception as e:
                    print(f"ERROR: {e}")
                    print("\n--- End of Traceback ---")
                    traceback.print_exc(file=sys.stdout)
                    conn.rollback()
                    raise

        ## close up shop
        cursor.close()
        # conn.commit()
        conn.close()

        return None

    def _unload_to_s3(
        self,
        sql_string: str,
        folder_name: str
    ) -> None:
        
        ## Construct query
        bucket_file_name = f"s3://{self.bucket.get_secret_value()}/{folder_name}/"
        authorization_string = f"""
            access_key_id '{self.aws_access_key_id.get_secret_value()}'
            secret_access_key '{self.aws_secret_access_key.get_secret_value()}'
        """

        unload_to_sql = f"""
            UNLOAD ('{sql_string.replace("'", "''")}')
            TO '{bucket_file_name}'
            FORMAT AS PARQUET
            {authorization_string}
            ;
        """

        ## Execute & commit
        if self.verbose:
            print(f"{unload_to_sql}")
            print("Unloading the data to S3! 🔃")

        self.execute_and_commit(unload_to_sql)

        if self.verbose:
            print("Successfully unloaded data to S3! ✅")
            print(f"Files can be found in 's3://{self.bucket.get_secret_value()}/{folder_name}/'")

        return None