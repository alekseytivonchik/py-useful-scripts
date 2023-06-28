import datetime
import logging
import os
import pathlib
import re
import requests
import subprocess as sp
from string import Template
import time

# Postgresql conf
pg_host = os.getenv('PG_HOST')
pg_port = os.getenv('PG_PORT')
pg_user = os.getenv('PG_USER')
pg_password = os.getenv('PG_PASSWD')
pg_db_names = os.getenv('DATABASE_LIST') # List of databases to be backuped
pg_backup_folder = '/root/backups' # Modify this value to your backps os path
pg_remove_threshold = 2 # Number of days older than backups will be removed
# Slack
slack_webhook = os.getenv('SLACK_WEBHOOK')
slack_report_tmpl = f'{pathlib.Path(__file__).parent.resolve()}/slack_report_tmpl.json'
slack_report_compl = '/root/slack_report.json'
slack_msg_color = '#2EB67D'
slack_jobs_status = 'Success'
# Logger conf
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s  %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Command exec
def exec_command(list):
    command_result = sp.run(list, text=True, capture_output=True)
    if command_result.stderr:
        logger.error(f'{command_result.stderr}')
    command_result.check_returncode()
    
# Slack
def slack_report_template(file_path,):
    current_date = datetime.datetime.now().strftime("%d-%m-%Y")
    with open(f'{slack_report_tmpl}', 'r') as tmpl:
        src = Template(tmpl.read())
    result = src.safe_substitute(
        {
            'slack_msg_color' : slack_msg_color,
            'current_date' : current_date,
            'pg_db_names' : pg_db_names,
            'slack_jobs_status' : slack_jobs_status,
            'pg_remove_threshold' : pg_remove_threshold
        }
    )
    with open(file_path, 'w') as slack_report:
        slack_report.write(result)
    return file_path

def slack_notify(webhook):
    with open(slack_report_template(slack_report_compl), 'r') as report:
        request_params = {'Content-Type': 'application/json'}
        request_data = report.read()
        requests.post(url = webhook, data = request_data.encode('utf-8'), params = request_params)

# Remove old backups
def pg_remove_backup(older_than_days, pg_db):
    day_in_sec = 86400
    current_time = time.time()
    list_of_files = os.listdir()
    
    for file in list_of_files:
        # Get the location of the file
        file_location = os.path.join(os.getcwd(), file)
        # Get last modify time of the file
        file_time = os.stat(file_location).st_mtime
        
        if re.search(rf'{pg_db}_backup_', f'{file}'):
            if file_time < current_time - day_in_sec * older_than_days:
                logger.info(f'Deleting backup: {file}')
                os.remove(file_location)
            else:
                logger.info(f'There are no backups to be deleted for database: {pg_db}')

# Generate backup file
def pg_backup_create(list):
    list = list.split()
    current_date = datetime.datetime.now().strftime("%d-%m-%Y")
    os.environ['PGPASSWORD'] = pg_password
    os.chdir(pg_backup_folder)
    
    for db_name in list:
        pg_backup_file = f"{db_name}_backup_{current_date}.sql.gz"
        pg_backup_cmd = [
            "pg_dump", 
            "-h", f"{pg_host}", 
            "-U", f"{pg_user}", 
            "-p", f"{pg_port}", 
            "-d", f"{db_name}", 
            "--format=custom", 
            "--compress=9", 
            f"--file={pg_backup_file}"
        ]
        
        try:
            logger.info(f'Starting backup database: "{db_name}"')
            exec_command(pg_backup_cmd)
            pg_remove_backup(3, db_name)
            logger.info(f'Backup database: "{db_name} successfully completed"')
        except Exception as e:
            global slack_jobs_status
            slack_jobs_status = 'Failed'
            global slack_msg_color
            slack_msg_color = '#E01E5A'
            logger.error(f'Backup database "{db_name}" failed with errors: {e}')
    
    try:
        slack_report_template(slack_report_compl)
        slack_notify(slack_webhook)
        logger.info('Successfully send message to Slack')
    except Exception as e:
        logger.error(f'Fail to send message to Slack with error(s): {e}')

def main():
    pg_backup_create(pg_db_names)
        

# Run PostgreSQL backup
if __name__ == '__main__':
    main()