# omeka_upload_data.py - a script to manage CSV data creation and upload with Omeka Classic installations

# (c) 2023 Steven J Baskauf. This program is released under a GNU General Public License v3.0 http://www.gnu.org/licenses/gpl-3.0
# Author: Steve Baskauf

# NOTE: The AWS access keys for writing to the S3 bucket must be stored in the ~/.aws/credentials file before running this script.
# To specify the directory subpath at run time, provide it as the first command line argument. Otherwise, the hard-coded value will be used.

# -------------------
# Imports
# -------------------
import os
import sys
import pandas as pd
from PIL import Image
import boto3 # AWS SDK for Python

# -------------------
# global variables
# -------------------
DATA_PATH = '../data/'
PYRAMIDAL_TIFFS_DIRECTORY_PATH = '/Users/baskausj/pyramidal_tiffs/'
UPLOAD_FILE_BASE_DIRECTORY_PATH = '/Users/baskausj/Downloads/bassett_raw_images/'
DIRECTORY_SUBPATH = 'zoo/kcz/master/'
S3_BUCKET = 'bassettassociates'

FORMAT_MAP = {
    'jpg': 'image/jpeg',
    'png': 'image/png',
    'tif': 'image/tiff',
    'gif': 'image/gif'
}

ORIGINAL_FORMAT_MAP = {
    'ph': 'photo',
    'sk': 'sketch',
    'pl': 'plan',
    'mo': 'model',
    'di': 'diagram'
}

CREATOR_MAP = {
    'ph': 'Bassett Associates',
    'sk': 'James H. Bassett',
    'pl': 'Bassett Associates',
    'mo': 'Bassett Associates',
    'di': 'Bassett Associates'
} 

TAGS_MAP = {
    'zoo': 'zoo',
    'cmp': 'campus',
    'cbd': 'downtown',
    'mrf': 'Muirfield',
    'pvt': 'private estate',
    'kcz': 'Kansas City',
    'ftw': 'Fort Wayne',
    'col': 'Columbus',
    'eri': 'Erie',
    'bin': 'Binder Park',
    'onu': 'ONU',
    'blu': 'Bluffton',
    'omr': 'OSU-Marion',
    'oli': 'OSU-Lima',
    'bgu': 'BGSU',
    'fin': 'Findlay',
    'lim': 'Lima',
    'bel': 'Bellfontaine',
    'tif': 'Tiffin',
    'man': 'Mansfield'
}

# If the DIRECTORY_SUBPATH is provided at run time, extract the subpath from the command line arguments
if len(sys.argv) > 1:
    DIRECTORY_SUBPATH = sys.argv[1]

# -------------------
# Functions
# -------------------

def move_pyramidal_tiffs_to_upload_subdirectory(directory_subpath: str, upload_file_base_directory_path: str, pyramidal_tiffs_directory_path: str) -> None:
    """Move the pyramidal tiffs to the upload directory."""

    # Find out if the upload directory exists and create it if not
    upload_directory_path = upload_file_base_directory_path + directory_subpath
    print('upload_directory_path:', upload_directory_path)
    if not os.path.exists(upload_directory_path):
        print('directory does not exist, creating it')
        os.makedirs(upload_directory_path)
    else:
        print('directory exists')

    # Invoke linux command to move the pyramidal tiffs to the upload directory
    # Note: the -n option prevents overwriting existing files
    os.system('mv -n ' + pyramidal_tiffs_directory_path + '*.tif ' + upload_file_base_directory_path + directory_subpath)
    os.system('mv -n ' + pyramidal_tiffs_directory_path + '*.jpg ' + upload_file_base_directory_path + directory_subpath)
    os.system('mv -n ' + pyramidal_tiffs_directory_path + '*.png ' + upload_file_base_directory_path + directory_subpath)
    os.system('mv -n ' + pyramidal_tiffs_directory_path + '*.gif ' + upload_file_base_directory_path + directory_subpath)

def aws_s3_upload(s3_bucket: str, directory_subpath: str, upload_file_base_directory_path: str) -> None:
    """Upload the files in the upload directory to the S3 bucket."""
    s3 = boto3.client('s3')
    local_path_root_path = upload_file_base_directory_path + directory_subpath

    # Loop through all files in the local directory
    for local_filename in os.listdir(local_path_root_path):
        # Skip the .DS_Store file (if on a Mac)
        if local_filename == '.DS_Store':
            continue
        s3_iiif_key = directory_subpath + local_filename
        print('Uploading to s3:', local_filename)
        s3.upload_file(local_path_root_path + local_filename, s3_bucket, s3_iiif_key)
        print('Done uploading to s3:', local_filename)
        print()

def generate_metadata_csv_for_omeka_upload(s3_bucket: str, directory_subpath: str, upload_file_base_directory_path: str, data_path: str) -> None:
    """Generate the metadata CSV file for the Omeka upload, primarily from information parsed from the file name."""

    # Read the empty CSV file into a dataframe to get the file headers, no NA values, read empty cells as empty strings
    upload_df = pd.read_csv(data_path + 'upload_headers.csv', na_filter=False, dtype=str)
    # Set the "Dublin Core:Identifier" column as the index
    upload_df = upload_df.set_index('Dublin Core:Identifier')

    # Get the list of files in the upload directory
    upload_file_directory_path = upload_file_base_directory_path + directory_subpath
    upload_file_list = os.listdir(upload_file_directory_path)

    # Set the base file URL for the S3 bucket
    upload_file_base_url = 'https://' + s3_bucket + '.s3.amazonaws.com/' + directory_subpath

    # Remove the .DS_Store file from the list (if on a Mac)
    if '.DS_Store' in upload_file_list:
        upload_file_list.remove('.DS_Store')

    # Loop through the files in the upload directory
    for file_name in upload_file_list:
        # Get the file name without the extension to use as the image_id
        image_id = os.path.splitext(file_name)[0]

        # Create a series (string datatype) for the row to be added to the dataframe, using the image_id as the index
        row_series = pd.Series(index=upload_df.columns, name=image_id, dtype=str)

        # Set the values of constant columns
        row_series['Dublin Core:Type'] = 'StillImage'
        row_series['Dublin Core:Rights'] = 'Available under a Creative Commons Attribution 4.0 International (CC BY 4.0) license'
        row_series['Dublin Core:Source'] = 'Bassett Associates files'
        row_series['Dublin Core:Publisher'] = 'James H. Bassett'
        
        # Create the upload URL from the base URL and the file name, then add it to the series as the upload_url value
        upload_url = upload_file_base_url + file_name
        row_series['upload_url'] = upload_url

        # Extract the original format type from the image_id, look it up in the map, and assign to the series. 
        # Example: zoo_kcz_chimp_ph_00, extract the next to last part ("ph"), look up "photograph".
        original_format_code = image_id.split('_')[-2]
        original_format = ORIGINAL_FORMAT_MAP[original_format_code]
        row_series['Item Type Metadata:Original Format'] = original_format

        # Extract the image dimensions from the file, construct the dimension string, and assign to the series
        image = Image.open(upload_file_directory_path + file_name)
        image_width, image_height = image.size
        dimensions = str(image_width) + 'x' + str(image_height)
        row_series['Item Type Metadata:Physical Dimensions'] = dimensions

        # Extract the file extension from the file name, look it up in the map, and assign to the series
        file_extension = file_name.split('.')[-1]
        file_format = FORMAT_MAP[file_extension]
        row_series['Dublin Core:Format'] = file_format

        # For original_format_codes "sk" and "pl", assign "en" as the language.
        if original_format_code in ['sk', 'pl']:
            row_series['Dublin Core:Language'] = 'en'
        else:
            row_series['Dublin Core:Language'] = ''

        # Set Title, Description, and Date values to empty strings
        row_series['Dublin Core:Title'] = ''
        row_series['Dublin Core:Description'] = ''
        row_series['Dublin Core:Date'] = ''

        # Set Creator value based on original_format_code
        row_series['Dublin Core:Creator'] = CREATOR_MAP[original_format_code]

        # Set tags base on first two parts of the identifier
        tags = image_id.split('_')[:2]
        # Construct comma-separated list of tags
        tag_list = []
        for tag in tags:
            if tag in TAGS_MAP:
                tag_list.append(TAGS_MAP[tag])
        tag_string = ','.join(tag_list)
        row_series['tags'] = tag_string

        # Add the series to the dataframe
        upload_df = upload_df.append(row_series)

    # Write the dataframe to a CSV file
    upload_df.to_csv(data_path + 'upload.csv', index=True, index_label='Dublin Core:Identifier')

# -------------------
# Main
# -------------------

print('moving pyramidal TIFFs to upload directory...')
move_pyramidal_tiffs_to_upload_subdirectory(DIRECTORY_SUBPATH, UPLOAD_FILE_BASE_DIRECTORY_PATH, PYRAMIDAL_TIFFS_DIRECTORY_PATH)

print('generating metadata CSV for Omeka upload...')
generate_metadata_csv_for_omeka_upload(S3_BUCKET, DIRECTORY_SUBPATH, UPLOAD_FILE_BASE_DIRECTORY_PATH, DATA_PATH)

print('uploading files to S3...')
aws_s3_upload(S3_BUCKET, DIRECTORY_SUBPATH, UPLOAD_FILE_BASE_DIRECTORY_PATH)

print('done')
