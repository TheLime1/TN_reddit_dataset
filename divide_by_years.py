import pandas as pd
import os
from datetime import datetime

# Create data directory if it doesn't exist
data_dir = 'data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

def process_file(filename, type_name):
    """
    Process a CSV file, dividing it by years and saving each year's data 
    as a separate file in the data directory.
    
    Parameters:
    filename (str): The name of the CSV file to process
    type_name (str): Either 'posts' or 'comments' to specify the type of data
    """
    print(f"Processing {filename}...")
    
    # Read the CSV file
    try:
        df = pd.read_csv(filename)
        print(f"  Successfully read {len(df)} records")
    except Exception as e:
        print(f"  Error reading file {filename}: {e}")
        return
    
    # Check if 'date' column exists
    if 'date' not in df.columns:
        print(f"  Error: 'date' column not found in {filename}")
        return
    
    # Extract year from date
    try:
        df['year'] = df['date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S').year)
        print(f"  Successfully extracted years")
    except Exception as e:
        print(f"  Error extracting years from dates: {e}")
        return
    
    # Group by year and save each group to a file
    years = df['year'].unique()
    print(f"  Found data from years: {sorted(years)}")
    
    for year in sorted(years):
        year_df = df[df['year'] == year]
        output_filename = os.path.join(data_dir, f"{type_name}_{year}.csv")
        year_df.drop('year', axis=1).to_csv(output_filename, index=False)
        print(f"  Saved {len(year_df)} {type_name} from {year} to {output_filename}")

def main():
    print("Starting to divide data by years...")
    
    # Process posts
    post_files = ['output_posts.csv', 'output_posts_after2022.csv']
    for file in post_files:
        if os.path.exists(file):
            process_file(file, 'posts')
        else:
            print(f"Warning: {file} not found, skipping")
    
    # Process comments
    comment_files = ['output_comments.csv', 'output_comments_after2022.csv']
    for file in comment_files:
        if os.path.exists(file):
            process_file(file, 'comments')
        else:
            print(f"Warning: {file} not found, skipping")
            
    print("Finished dividing data by years!")

if __name__ == "__main__":
    main()