import os
import pandas as pd
import glob

def fix_urls_in_csv(file_path):
    """
    Fix URLs in a CSV file to follow the Reddit format:
    https://www.reddit.com/r/Tunisia/comments/{id}/
    """
    try:
        # Read the CSV file
        df = pd.read_csv(file_path)
        
        # Check if the file has content and 'id' column exists
        if df.empty or 'id' not in df.columns:
            print(f"Skipping {file_path} (empty or missing id column)")
            return 0
            
        # Update the URLs based on the id column
        df['url'] = df['id'].apply(lambda x: f"https://www.reddit.com/r/Tunisia/comments/{x}/")
        
        # Save the updated DataFrame back to the CSV
        df.to_csv(file_path, index=False)
        
        print(f"Fixed URLs in {file_path}")
        return len(df)
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return 0

def main():
    # Get all post CSV files in the data directory
    csv_pattern = os.path.join('data', 'posts_*.csv')
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        print("No post CSV files found in the data directory.")
        return
    
    total_fixed = 0
    for file_path in csv_files:
        fixed_count = fix_urls_in_csv(file_path)
        total_fixed += fixed_count
    
    print(f"URL fixing complete! Fixed {total_fixed} URLs across {len(csv_files)} files.")

if __name__ == "__main__":
    main()