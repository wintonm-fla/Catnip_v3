from pydantic import BaseModel, SecretStr

import boto3 
import botocore
from botocore.response import StreamingBody

from typing import List, Dict, Any, Union
from io import BytesIO

from concurrent.futures import ThreadPoolExecutor

class FLA_S3(BaseModel):

    ## S3 Bucket Info
    aws_access_key_id: SecretStr
    aws_secret_access_key: SecretStr
    bucket: SecretStr

    max_pool_connections: int = 10    # max amount of threads to use

    ##############################################
    ### CLASS FUNCTIONS ##########################
    ##############################################

    def upload_file(
        self,
        data: BytesIO,
        folder: str,
        filename: str,
        s3_client: boto3.client = None
    ) -> bool:

        ## create key
        key = f"{folder}/{filename}"

        ## create connection, if necessary
        if not s3_client:
            s3_client = self._connect_to_s3()

        ## upload file
        ## return boolean value to indicate success/failure
        try:
            s3_client.put_object(
                Bucket=self.bucket.get_secret_value(),
                Key=key,
                Body=data
            )
            print(f"Successfully uploaded {filename} to 's3://{key}'")
            return True
        
        except Exception as e:
            print(f"Failed to upload {filename} to 's3://{key}'")
            print(f"Error: {e}")
            return False

    def upload_files(
        self,
        folder: str,
        file_payloads: List[Dict[str, str | BytesIO]]
    ) -> bool:

        """
        Upload multiple files to an S3 folder in parallel.

        Args:
            folder (str): The S3 folder to upload files to.
            file_payloads (List[Dict[str, str | BytesIO]]): 
                A list of dictionaries, each with:
                    - "filename": The name of the file (str)
                    - "data": The file content as a BytesIO object

        Returns:
            bool: True if all files uploaded successfully, False otherwise.

        Notes:
            - Uses ThreadPoolExecutor for parallel uploads (max threads set by self.max_pool_connections).
            - Each file is uploaded using the upload_file method.
            - Prints a summary of upload results.
            - If any file fails to upload, the function returns False.
            - The S3 key for each file is constructed as: 
                '{folder}/{filename}'
            - Ensure that each dictionary in file_payloads contains both "filename" and "data" keys.
            - The function assumes that the S3 client and credentials are valid.
        """
        ## create s3 client
        s3_client = self._connect_to_s3()

        ## create lists for executor
        num_files = len(file_payloads)
        filename_list = [item["filename"] for item in file_payloads if "filename" in item]
        data_stream_list = [item["data"] for item in file_payloads if "data" in item]
        folder_list = [folder] * num_files
        client_list = [s3_client] * num_files
        all_successful = True

        with ThreadPoolExecutor(max_workers=self.max_pool_connections) as executor:
            results = executor.map(
                self.upload_file,    # Function to call
                data_stream_list,   # Argument for 'data'
                folder_list,        # Argument for 'folder'
                filename_list,      # Argument for 'filename'
                client_list         # Argument for 's3_client'
            )
            
            for result in results:
                if not result:
                    all_successful = False

        if all_successful:
            print(f"All {num_files} files successfully uploaded to folder '{folder}'.")
        else:
            print(f"Some files failed to upload to folder '{folder}'. Check logs.")
            
        return all_successful

    def download_file(
        self,
        filename: str,
        folder: str = None,
        s3_client: boto3.client = None,
        return_as_streaming_body: bool = True
    ) -> Dict | StreamingBody:

        ## create key
        if folder is not None:
            key = f"{folder}/{filename}"
        else:
            key = f"{filename}"

        ## create connection, if necessary
        if not s3_client:
            s3_client = self._connect_to_s3()

        ## download file
        ## return None if fail to download
        try:
            response = s3_client.get_object(
                Bucket=self.bucket.get_secret_value(),
                Key=key,
            )
            print(f"Successfully downloaded 's3://{key}'")
            if return_as_streaming_body:
                return response['Body']
            else:
                return response
        
        except Exception as e:
            print(f"Failed to download 's3://{self.bucket.get_secret_value()}/{key}'")
            print(f"Error: {e}")
            return None

    def download_files(
        self,
        filenames: List[str],
        folder: str = None,
        return_as_streaming_body: bool = True
    ) -> Dict[str, Dict | StreamingBody]:

        ## create s3 client
        s3_client = self._connect_to_s3()

        ## create lists for executor
        num_files = len(filenames)
        folder_list = [folder] * num_files
        client_list = [s3_client] * num_files
        return_as_streaming_body_list = [return_as_streaming_body] * num_files
        download_results: Dict[str, Union[StreamingBody, Dict, bool]] = {}

        with ThreadPoolExecutor(max_workers=self.max_pool_connections) as executor:
            results = executor.map(
                self.download_file,
                filenames,
                folder_list,
                client_list,
                return_as_streaming_body_list
            )
            
            for i, result_item in enumerate(results):
                original_filename = filenames[i] # Get the corresponding filename
                download_results[original_filename] = result_item
        
        successful_downloads = sum(1 for res in download_results.values() if res is not None)
        failed_downloads = num_files - successful_downloads
        print(f"Finished download attempts for {num_files} files from folder '{folder}'.")
        print(f"Successful: {successful_downloads}, Failed: {failed_downloads}")

        return download_results
    
    def get_all_filenames_in_folder(self, folder: str) -> List[str]:
        
        s3_client = self._connect_to_s3() 

        response = s3_client.list_objects_v2(
            Bucket=self.bucket.get_secret_value(), 
            Prefix=folder
        )
        results = ['/'.join(x['Key'].split('/')[1:]) for x in response['Contents']]
        
        s3_client.close()

        return results
    
    def get_folder_metadata(self, folder: str) -> Dict[str, Any]:

        metadata = {}
        s3_client = self._connect_to_s3()

        response = s3_client.list_objects_v2(
            Bucket=self.bucket.get_secret_value(), 
            Prefix=folder
        )

        files = [{'filepath': x['Key'], 'size': x['Size']} for x in response['Contents']]
        metadata['num_files'] = len(files)

        total_size = sum(file['size'] for file in files)
        metadata['total_file_size_mb'] = round(total_size / (1024 * 1024), 2)

        s3_client.close()

        return metadata

    ##############################################
    ### CONNECTION FUNCTIONS #####################
    ##############################################
    
    def _connect_to_s3(self):

        client_config = botocore.config.Config(
            max_pool_connections= self.max_pool_connections
        )

        return boto3.client(
            "s3",
            aws_access_key_id = self.aws_access_key_id.get_secret_value(),
            aws_secret_access_key = self.aws_secret_access_key.get_secret_value(),
            config = client_config
        )