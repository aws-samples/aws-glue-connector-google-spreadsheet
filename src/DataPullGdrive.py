"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0
"""
import sys
import logging
import pandas as pd
import json
from datetime import datetime
from os.path import join
import awswrangler as wr
import boto3
from awsglue.utils import getResolvedOptions
from googleapiclient.discovery import build
from google.oauth2 import service_account

# start your logging session
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Collect your script variables
args = getResolvedOptions(sys.argv,
                          ['google_secret_name',
                           'google_secret_region',
                           'google_spreadsheet_id',
                           'google_spreadsheet_tab',
                           'bucket',
                           'folder',
                           'filename',
                           'output_format',
                           'compression']
                          )


# convert param to boolean
def str_to_bool(val):
    return val.lower() in ['true', '1', 't', 'y', 'yes']


# Method to save data in Amazon S3 Bucket
def save_file(df, bucket, folder='data',
              filename='filename', output_format='csv',
              compression=False):
    """
    Save your pandas dataframe in your Amazon S3 bucket.   
    This method requires df and bucket as mandatory variables.
    Provide the rest of variables as job parameters or use the default values.
    
    Modify the script to handle more text file separators.
    Extend the script to include more compression algorithms.
    
    Include the subfolder structure directly in your job variable folder.
    Use the following pattern: folder/subfolder1/subfolder2/...
    """
    # Create the file path with bucket, folder and filename
    subpath_args = [bucket, folder, filename]
    sub_path = '/'.join(subpath_args)
    # Add the creation time in your filename
    path_date_args = [sub_path, datetime.now().strftime('%Y_%m_%d_%H_%M_%S')]
    path_date = '_'.join(path_date_args)
    # Add the S3 url into your path
    path = 's3://'+path_date
    if output_format == 'csv':
        """
        By default this routine uses tab separator.
        Define a new job variable for different separator.
        Insert here your code to handle different types of separators
        """
        separator = '\t'
        # Add the csv file extension
        path_ext = path+'.csv'
        if (str_to_bool(compression)):
            # Include your logic to handle more compression algorithms
            """
            Insert here your code to apply more textfile compression methods.
            """
            comp_opts = 'zip'
            # Add the compression to your filename
            path_ext = path_ext+'.'+comp_opts
        else:
            comp_opts = None
        # Save your file with awswrangler package
        wr.s3.to_csv(
            df=df,
            path=path_ext,
            compression=comp_opts,
            index=False
        )
    else:
        # Add the parquet file extension
        path_ext = path+'.parquet'
        if (str_to_bool(compression)):
            """
            Insert here your code to apply more Parquet compression methods.
            """
            comp_opts = 'snappy'
            # Add the compression to your filename
            path_ext = path_ext+'.'+comp_opts
        else:
            comp_opts = None
        # Save your file with awswrangler package
        wr.s3.to_parquet(
            df=df,
            path=path_ext,
            compression=comp_opts,
            index=False
        )
    return


# Retrieve credentials from AWS Secrets Manager
# replace this value with your secret name
secret_name = args['google_secret_name']
region_name = args['google_secret_region']  # modify to your region
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=args['google_secret_region']
)

get_secret_value_response = client.get_secret_value(
    SecretId=args['google_secret_name']
)

secret = get_secret_value_response['SecretString']
secret = json.loads(secret)

# Create credentials object with google.oauth2.
# Use those scopes for to connect to Google Drive.
# More information:
# https://google-auth.readthedocs.io/en/master/reference/google.oauth2.service_account.html
credentials = service_account.Credentials.from_service_account_info(
    secret,
    scopes=['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets'])

# Open connection with googleapiclient and create object connection.
# More information: https://developers.google.com/sheets/api/quickstart/python
service = build('sheets', 'v4', credentials=credentials)
sheet = service.spreadsheets()

# Read google object
# More information:
# https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get
result = sheet.values().get(
                       spreadsheetId=args['google_spreadsheet_id'],
                       range=args['google_spreadsheet_tab']).execute()
data = result.get('values')

# save data into a dataframe
data = pd.DataFrame(data[1:], columns=data[0])
bucket = args['bucket']
folder = args['folder']
filename = args['filename']
output_format = args['output_format']
compression = args['compression']
save_file(data, bucket, folder, filename, output_format, compression)
