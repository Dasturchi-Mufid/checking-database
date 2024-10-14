import fdb
import requests
import logging
import os
from datetime import datetime, timedelta
from time import sleep
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv


logging.basicConfig(
    filename='scheduler.log',  # Log file
    level=logging.INFO,  # Log level (INFO, DEBUG, WARNING, etc.)
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv('TOKEN')
TELEGRAM_CHAT_ID = os.getenv('CHANNEL')
API = os.getenv('API')
d = os.getenv('D')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
CHARSET = os.getenv('CHARSET')


def get_db_connection(database_path,db_user,db_password,charset):
    return fdb.connect(dsn=database_path, user=db_user, password=db_password, charset=charset)

def get_time_range():
    # Get today's date
    now = datetime.now()

    # Calculate yesterday's date at 20:00
    yesterday_20 = (now - timedelta(days=1)).replace(hour=21, minute=0, second=0, microsecond=0)

    # Calculate today's date at 08:00
    today_08 = now.replace(hour=8, minute=0, second=0, microsecond=0)

    return yesterday_20.strftime('%Y-%m-%d %H:%M:%S'), today_08.strftime('%Y-%m-%d %H:%M:%S')


def send_to_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }
    response = requests.post(url, data=data)
    
    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 1))
        sleep(retry_after)  # Wait before retrying
        return send_to_telegram(message)
    
    return response.status_code == 200



start_time, end_time = get_time_range()

def perform_task():
    for f in d.keys():
        DATABASE_PATH = f"{API}/{f}base.fdb"

        conn = get_db_connection(database_path=DATABASE_PATH,db_user=DB_USER,db_password=DB_PASSWORD,charset=CHARSET)
        cursor = conn.cursor()
        query = f"""
        SELECT d.id, f.fio, l.NOMI,d.MODEL, d.MARKA, d.SERIYA, d.VAQT
        FROM DEVICEINFO d
        INNER JOIN FOYDALANUVCHI f ON f.id = d.USER_ID
        INNER JOIN LAVOZIM l ON l.id=f.LAVOZIM_ID 
        WHERE d.vaqt BETWEEN '{start_time}' AND '{end_time}'
        """
        cursor.execute(query)

        items = cursor.fetchall()
        cursor.close()
        conn.close()
        
        for item in items:
            time = item[6].strftime('%d/%m/%y %H:%M:%S')
            message = f"{d[f]}\n👤Name: {item[1]},\n💵 Position: {item[2]}\n📳 Model: {item[3]},\n📳 Brand: {item[4]},\n🈁 Serial: {item[5]},\n🕛 Time: {time}"
            send_to_telegram(message)
            sleep(1)

# Function to start the scheduler
# def start_scheduler():
#     scheduler = BackgroundScheduler()
#     scheduler.add_job(perform_task, "cron", hour=11, minute=24)
#     scheduler.start()

# Start the scheduler when the app starts
# @app.on_event("startup")
# def startup_event():
#     start_scheduler()


# if __name__=='__main__':
#     start_scheduler()

scheduler = BackgroundScheduler()

# Schedule the task to run every day at 7:30 AM
scheduler.add_job(perform_task, 'cron', hour=14, minute=16)

# Start the scheduler
scheduler.start()

try:
    while True:
        sleep(1)  # Keep the main thread alive
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()  # Shut down the scheduler on exit
except Exception as e:
    logging.error(f"Error running the scheduled task: {e}")