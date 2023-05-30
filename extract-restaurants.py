import os
import tqdm
import pandas as pd

def get_category(meta):
    """
    A function to extract the category from metadata.
    """
    l = []
    for i in range(len(meta['category'])):
        try:
            category = meta['category'][i][0].lower()
        except:
            category = None
        l.append(category)
    return l

# Set the working directory to the repository location
os.chdir('/work/GoogleLocalData2021')

print('Reading Metadata...')
# Read the metadata from the JSON file into a pandas DataFrame
meta = pd.read_json('/work/GoogleLocalData2021/data/meta-California.json', lines=True)
meta['category'] = get_category(meta)  # Add a new column 'category' to the metadata DataFrame
meta.dropna(subset='category', inplace=True)
meta = meta[meta['category'].str.contains('restaurant')]
restaurants_id = meta.gmap_id # Extract ids of restaurants

print('Saving Metadata...')
# Save the metadata DataFrame as a pickle file
meta.to_pickle('./data/meta.pkl')

chunk_size = 100000
dataframes = []

print('Extracting Restaurant Reviews...')

# Open the restaurant review JSON file
with open('./data/review-California.json', 'r') as file:
    # Read the JSON file in chunks and process each chunk
    for chunk in tqdm.tqdm(pd.read_json(file, lines=True, chunksize=chunk_size), total=706, desc='Processing'):
        # Filter and process the chunk as needed
        filtered_data = chunk[chunk['gmap_id'].isin(restaurants_id)]  # Filter the chunk for restaurant IDs
        filtered_data = filtered_data.dropna(subset=['text']) # Drop the reviews that contain no text
        dataframes.append(filtered_data)  # Append the filtered chunk to the list of dataframes

print('Concatenating Chunks...')
# Concatenate all the filtered dataframes into a single dataframe
reviews = pd.concat(dataframes, ignore_index=True)

print('Saving Reviews...')
# Save the restaurant reviews dataframe as a pickle file
reviews.to_pickle('./data/restaurant-reviews.pkl')

print('Done!')
