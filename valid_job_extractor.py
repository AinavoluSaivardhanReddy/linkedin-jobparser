from jobParser import process_linkedin_jobs
from scrape import get_job_details
import pandas as pd
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument("--time", type=int, help="Time window of search")
parser.add_argument("--exp", type=int, help="maximum experience allowed for the jobs ")
parser.add_argument("--size", type=str, help="size of the flan-t5 model used to parse the job experience")

args = parser.parse_args()

source_file_name = 'linkedinjobs6.csv'
target_file = 'validjobs.csv'

max_experience = 2              # the maximum experience allowed for the jobs
if args.exp != None:
    max_experience = args.exp

filter_time = 48 * 60 * 60      # filter time is specified in milliseconds
if args.time != None:
    filter_time = args.time * 60 * 60

model_size = "small"            # size of the flan-t5 model used to parse job experience
if args.size != None:
    model_size = args.size

# This is a list of roles that are searched for on linkedin
roles = ['Full Stack', 'Software Engineer', 'Software Developer', 'Founding Engineer', 'Frontend', 'Backend', 'Data Engineer']

get_job_details(source_file_name, roles, filter_time)
process_linkedin_jobs(source_file_name, model_size)

df = pd.read_csv(source_file_name)
df['experience'] = pd.to_numeric(df['experience'], errors='coerce')
df = df[df['experience'] <= max_experience]
df['target_url'] = df['job_id'].apply(lambda x: f"https://www.linkedin.com/jobs/view/{x}")

if os.path.exists(target_file):
    existing_jobs = pd.read_csv(target_file)
    df = df[~df['job_id'].isin(existing_jobs['job_id'])]
    df.to_csv(target_file, mode='a', index=False, header=False, encoding='utf-8', lineterminator='\n')
else:
    df.to_csv(target_file, mode='w', index=False, header=True, encoding='utf-8', lineterminator='\n')



