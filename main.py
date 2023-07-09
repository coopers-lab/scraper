import json
import os
import time

CONFIG_FILE = "config.json"

def first_time_setup():
    print('First run detected!')
    print('Installing Modules!')
    os.system('pip install -r requirements.txt')

    data = {"first_time": False}
    with open(CONFIG_FILE, "w") as f:
        json.dump(data, f)
    print('Done!')
    print('Continuing...')
    try:
        os.system('cls')
    except:
        os.system('clear')
    time.sleep(3)


# Check if the config.json file exists
if not os.path.exists(CONFIG_FILE):
    first_time_setup()


from bose.launch_tasks import launch_tasks
from src import tasks_to_be_run
from colorama import Back
import os
import csv
import re
import requests
from colorama import Fore, Back, init
import csv
import threading
from queue import Queue
from validate_email_address import validate_email

init()

if __name__ == "__main__":
    print(f'{Back.LIGHTBLUE_EX}  INFO  {Back.RESET} Scraping Google Maps for Websites...')
    launch_tasks(*tasks_to_be_run)
    print(f'{Back.LIGHTGREEN_EX}  INFO  {Back.RESET} Done Scraping Google Maps for Websites...')
    print(f'{Back.LIGHTBLUE_EX}  INFO  {Back.RESET} Extracting Websites...')

    def extract_columns(input_file, output_file):
        with open(input_file, 'r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            headers = ['title', 'website']

            with open(output_file, 'w', newline='') as output:
                writer = csv.DictWriter(output, fieldnames=headers)
                writer.writeheader()

                for row in reader:
                    extracted_row = {key: row[key] for key in headers}
                    writer.writerow(extracted_row)

            print(f"{Back.LIGHTGREEN_EX}  INFO  {Back.RESET} Extraction Complete! Extracted data saved to '{output_file}'")


    input_file = os.path.join(os.getcwd(), 'output', 'all.csv')
    output_file = 'extracted_data.csv'

    extract_columns(input_file, output_file)


    print(f'{Back.LIGHTBLUE_EX}  INFO  {Back.RESET} Crawling websites for emails...')
    def extract_emails_from_text(text):
        # Regular expression pattern to match email addresses
        pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'
        return re.findall(pattern, text)

    def crawl_websites(csv_file):
        # Open the input CSV file
        with open(csv_file, 'r') as file:
            reader = csv.DictReader(file)
            
            # Create a new CSV file to store the extracted emails
            with open('data.csv', 'w', newline='') as output_file:
                fieldnames = ['Title', 'Email']
                writer = csv.DictWriter(output_file, fieldnames=fieldnames)
                writer.writeheader()
                
                extracted_emails = set()  # Set to store unique email addresses
                
                # Iterate over each row in the input CSV
                for row in reader:
                    title = row['title']
                    website = row['website']
                    
                    try:
                        # Send a GET request to the website
                        response = requests.get(website)
                        
                        # Extract email addresses from the response text
                        emails = extract_emails_from_text(response.text)
                        
                        # Write the extracted emails to the output CSV
                        for email in emails:
                            if email not in extracted_emails:
                                extracted_emails.add(email)
                                writer.writerow({'Title': title, 'Email': email})
                                print(f'{Back.GREEN}  HIT  {Back.RESET} {email}')
                            
                    except requests.exceptions.RequestException:
                        # Handle any errors that occur during the request
                        print(f"{Back.RED}  NULL  {Back.RESET} Error crawling website: {website}")
        
        print(f"{Back.LIGHTGREEN_EX}  INFO  {Back.RESET} Email extraction and duplicate removal completed. Results saved in data.csv")

    # Example usage
    crawl_websites('extracted_data.csv')
    print(f'{Back.LIGHTBLUE_EX}  INFO  {Back.RESET} Removing duplicate lines and verifiying emails...')

    def remove_duplicate_lines(file_path):
        lines_seen = set()
        duplicate_lines = []

        # Read the file and find duplicate lines
        with open(file_path, 'r', newline='') as file:
            reader = csv.reader(file)
            for line_num, line in enumerate(reader, start=1):
                line_str = ','.join(line)  # Convert line list to string
                if line_str in lines_seen:
                    duplicate_lines.append((line_str, line_num))
                else:
                    lines_seen.add(line_str)

        # Remove duplicate lines from the file
        if duplicate_lines:
            temp_file_path = file_path + '.tmp'
            with open(file_path, 'r', newline='') as file, open(temp_file_path, 'w', newline='') as temp_file:
                reader = csv.reader(file)
                writer = csv.writer(temp_file)
                for line in reader:
                    line_str = ','.join(line)
                    if line_str not in lines_seen:
                        writer.writerow(line)

            # Replace the original file with the temporary file
            os.remove(file_path)
            os.rename(temp_file_path, file_path)

        return duplicate_lines

    # Usage example
    file_path = 'data.csv' # Replace with your file path
    duplicates = remove_duplicate_lines(file_path)

    if duplicates:
        print(f"{Back.RED}  INFO  {Back.RESET} Duplicate lines found:")
        for line, line_num in duplicates:
            print(f"Line {line_num}: {line}")
        print(f'{Back.LIGHTBLUE_EX}  INFO  {Back.RESET} Continuing...')
    else:
        print(f"{Back.LIGHTGREEN_EX}  INFO  {Back.RESET} No duplicate lines found.")
        print(f'{Back.LIGHTBLUE_EX}  INFO  {Back.RESET} Continuing...')
    # Worker function to validate emails
    def validate_emails_worker(email_queue, result_queue):
        while True:
            email = email_queue.get()
            if email is None:  # Exit condition for worker thread
                break
            is_valid = validate_email(email)
            result_queue.put((email, is_valid))
            email_queue.task_done()

    def validate_emails(file_path):
        email_queue = Queue()
        result_queue = Queue()
        num_threads = 50  # Number of worker threads change if your pc is slow

        # Create worker threads
        threads = []
        for _ in range(num_threads):
            t = threading.Thread(target=validate_emails_worker, args=(email_queue, result_queue))
            t.start()
            threads.append(t)

        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                email_queue.put(row['Email'])

        # Wait for all emails to be processed
        email_queue.join()

        # Signal worker threads to exit
        for _ in range(num_threads):
            email_queue.put(None)

        # Wait for worker threads to exit
        for t in threads:
            t.join()

        # Collect results
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        return results

    # Usage example
    results = validate_emails(file_path)

    # Display results
    for email, is_valid in results:
        result_text = "Valid" if is_valid else "Invalid"
        print(f"Email: {email} - {result_text}")

    print(f"{Back.GREEN}  INFO  {Back.RESET} Completed successfully! Emails are saved to 'data.csv'")


    


