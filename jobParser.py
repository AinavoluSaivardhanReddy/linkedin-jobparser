import os
import pandas as pd
from bs4 import BeautifulSoup
import sys
import contextlib
import re
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
import sys
import time
import threading
import logging
from transformers import T5ForConditionalGeneration, T5Tokenizer

logging.disable(logging.CRITICAL + 1)


def find_highest_lowest(s):

    numbers = re.findall(r'\d+\+|\d+-\d+|\d+', s)
    processed_numbers = []
    for num in numbers:
        if '-' in num:
            start, end = map(int, num.split('-'))
            processed_numbers.extend(range(start, end + 1))
        elif '+' in num:
            processed_numbers.append(float(num.replace('+', '')) + 0.5)
        else:
            processed_numbers.append(int(num))
    
    if not processed_numbers:
        return 0, 0

    return max(processed_numbers), min(processed_numbers)

progress_lock = threading.Lock()
    
def get_model(size):
    if size == "medium": size = "base"
    print(f"Parsing job experience information with flan-t5-{size} model...\n")
    tokenizer = T5Tokenizer.from_pretrained(f"google/flan-t5-{size}", legacy=True)
    model = T5ForConditionalGeneration.from_pretrained(f"google/flan-t5-{size}")
    return tokenizer, model

def parse_experience(tokenizer, model, job_description, question):
    input_text = f"Job Description: {job_description} question={question}, Answer:"
    input_ids = tokenizer(input_text, return_tensors="pt").input_ids
    outputs = model.generate(input_ids, max_new_tokens=20, max_length=50)
    result = BeautifulSoup(tokenizer.decode(outputs[0]), "html.parser").get_text(separator=' ', strip=True)
    return result

def progress_bar(current, total):
    progress_length = 100
    percentage = current / total
    num_equals = int(percentage * progress_length)
    progress = '#' * num_equals + '>' + '=' * (progress_length - num_equals)
    bar = f"[{progress}] {percentage * 100:.2f}%"
    sys.stdout.write('\r' + bar)
    sys.stdout.flush()

def process_row(row, index, total_length, current_count, tokenizer, model):
    if pd.notnull(row["experience"]):
        with current_count.get_lock():
            current_count.value += 1
        progress_bar(current_count.value, total_length)
        return index, None
    question = "How many years of work experience are required for this role according to the job description? Answer with a number."
    job_description = row["description"]
    job_description = ' '.join(job_description.split())
    text = parse_experience(tokenizer, model, job_description, question)
    _, low = find_highest_lowest(text)
    with current_count.get_lock():
        current_count.value += 1
    progress_bar(current_count.value, total_length)
    return index, low


def process_linkedin_jobs(file_name, model_size="medium"):
    df = pd.read_csv(file_name)
    if 'experience' not in df.columns:
        df['experience'] = pd.Series(dtype='int64')
    
    current_count = multiprocessing.Value('i', 0)

    tokenizer, model = get_model(model_size)
    number_of_threads = 10

    with ThreadPoolExecutor(max_workers=number_of_threads) as executor:
        futures = {executor.submit(process_row, row, index, len(df), current_count, tokenizer, model): index for index, row in df.iterrows()}

        while futures:
            completed_futures = [f for f in futures if f.done()]
            for future in completed_futures:
                index, result = future.result()
                if result is not None:
                    df.at[index, 'experience'] = result
                del futures[future]
                progress_bar(len(df) - len(futures), len(df))
            
            time.sleep(0.1)

    df.to_csv(file_name, index=False)
    sys.stdout.write('\r')
