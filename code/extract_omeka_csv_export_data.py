# extract_omeka_csv_export_data.py - a script to extract the identifier data from the Omeka CSV export file

# (c) 2023 Steven J Baskauf. This program is released under a GNU General Public License v3.0 http://www.gnu.org/licenses/gpl-3.0
# Author: Steve Baskauf

# -------------------
# Imports
# -------------------

from typing import List, Dict, Tuple, Any, Optional
import pandas as pd

# -------------------
# global variables
# -------------------

DATA_PATH = '../data/'

# -------------------
# functions
# -------------------

def csv_read(path: str, **kwargs) -> pd.DataFrame:
    """Loads a CSV table into a Pandas DataFrame with all cells as strings and blank cells as empty strings

    Keyword argument:
    rows -- the number of rows of the table to return when used for testing. When omitted, all rows are returned.
    """
    dataframe = pd.read_csv(path, na_filter=False, dtype=str)
    if 'rows' in kwargs:
        return dataframe.head(kwargs['rows']).copy(deep=True)
    else:
        return dataframe

# -------------------
# main
# -------------------

print('extracting Omeka CSV export data')

# Read the CSV export file into a Pandas DataFrame
export_df = csv_read(DATA_PATH + 'export.csv')
# Set the index of the DataFrame to the item identifier
export_df = export_df.set_index('Dublin Core:Identifier')

# Read the exsiting Omeka item identifiers data into a Pandas DataFrame
identifiers_df = csv_read(DATA_PATH + 'identifiers.csv')
# Set the index of the DataFrame to the item identifier
identifiers_df = identifiers_df.set_index('identifier')

# Loop through the rows of the identifiers DataFrame and find rows that are missing Omeka file identifiers
for index, row in identifiers_df.iterrows():
    if row['omeka_id'] == '':
        # Get the index of the row
        identifier = row.name
        print(identifier)

        # The Omeka export row index is the same as the item identifier
        # Get the omeka_id value from the Omeka export row with the same index as the identifier
        omeka_file_url = export_df.loc[identifier]['file']
        # Extract the Omeka file name from the URL
        omeka_file_name = omeka_file_url.split('/')[-1]
        # Extract the Omeka file identifier from the file name
        omeka_file_identifier = omeka_file_name.split('.')[0]
        # Update the identifiers DataFrame with the Omeka file identifier
        identifiers_df.loc[identifier, 'omeka_id'] = omeka_file_identifier

        # Get the Item Id value from the Omeka export row with the same index as the identifier
        omeka_item_id = export_df.loc[identifier]['Item Id']
        # Update the item_id value in the identifiers_df row with the Omeka item identifier
        identifiers_df.loc[identifier, 'item_id'] = omeka_item_id

# Write the updated identifiers DataFrame to a CSV file
identifiers_df.to_csv(DATA_PATH + 'identifiers.csv', index=True)
print()

# Copy the uploaded data into the image records CSV
print('copying uploaded data into image records CSV')

# Open the upload.csv file to be appended
upload_file_df = csv_read(DATA_PATH + 'upload.csv')

# Open the items.csv file to be read
items_file_df = csv_read(DATA_PATH + 'items.csv')

# Append the upload data to the end of the items dataframe
items_file_df = items_file_df.append(upload_file_df, ignore_index=True)

# Write the updated items dataframe to a CSV file
items_file_df.to_csv(DATA_PATH + 'items.csv', index=False)

print('done')
