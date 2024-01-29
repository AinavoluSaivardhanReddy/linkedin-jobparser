import pandas as pd

# File paths for your CSV files
csv_file_1 = 'linkedinjobs5.csv'
csv_file_2 = 'linkedinjobs6.csv'

# Load the CSV files
df1 = pd.read_csv(csv_file_1)
df2 = pd.read_csv(csv_file_2)

# Identify the unique job_ids in both dataframes
unique_job_ids_df2 = set(df2['job_id']) - set(df1['job_id'])

# Filter rows with these unique job_ids

unique_rows_df2 = df2[df2['job_id'].isin(unique_job_ids_df2)]

# Concatenate the unique rows
combined_unique_rows = pd.concat([ unique_rows_df2])

# Save the combined data to a new CSV file
output_file = 'unique_jobs_combined.csv'
combined_unique_rows.to_csv(output_file, index=False)
