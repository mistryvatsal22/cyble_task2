import json
import mysql.connector
import threading
from queue import Queue

# Configuration
BATCH_SIZE = 1000
NUM_WORKERS = 2
FILE_PATH = 'C:/Users/vatsa/Documents/Python/ndjson_mysql_project/sample_ndjson.txt'

db_config = {
    "host": "localhost",
    "user": "root",
    "password": "vatsal2208",
    "database": "ndjsondb"
}

# Thread-safe queue for batch data
data_queue = Queue(maxsize=10)

# Worker function
def db_worker(worker_id=0):
    print(f"[Worker {worker_id}] Starting DB worker thread.")
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    while True:
        batch = data_queue.get()
        if batch is None:
            print(f"[Worker {worker_id}] Received stop signal. Exiting.")
            data_queue.task_done()
            break
        try:
            cursor.executemany("INSERT INTO `table` (name, age) VALUES (%s, %s)", batch)
            conn.commit()
            print(f"[Worker {worker_id}] Inserted batch of {len(batch)} records.")
        except Exception as e:
            print(f"[Worker {worker_id}] [Error] Insert failed: {e}")
        data_queue.task_done()
    cursor.close()
    conn.close()
    print(f"[Worker {worker_id}] Connection closed.")

def ensure_table_exists():
    print("[Main] Ensuring table exists...")
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS `table` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            age INT
        )
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("[Main] Table check/creation complete.")

# Main function to read file and feed queue
def process_file():
    print(f"[Main] Reading file: {FILE_PATH}")
    batch = []
    total_records = 0
    try:
        with open(FILE_PATH, 'r', encoding='utf-8', errors='replace') as file:
            for line_num, line in enumerate(file, 1):
                try:
                    record = json.loads(line.strip())
                    name = record.get('name')
                    age = record.get('age')
                    if name is not None and age is not None:
                        batch.append((name, age))
                        total_records += 1
                        if len(batch) == BATCH_SIZE:
                            data_queue.put(batch)
                            print(f"[Main] Queued batch of {BATCH_SIZE} records at line {line_num}.")
                            batch = []
                except Exception as e:
                    print(f"[Main] [Error] JSON parse failed at line {line_num}: {e}")
            if batch:
                data_queue.put(batch)
                print(f"[Main] Queued final batch of {len(batch)} records.")
    except Exception as e:
        print(f"[Main] [Error] File open failed: {e}")
        return

    print(f"[Main] Finished reading file. Total records queued: {total_records}")
    # Send poison pill to stop threads
    for _ in range(NUM_WORKERS):
        data_queue.put(None)
    print("[Main] Stop signals sent to workers.")

# Call this before starting worker threads
ensure_table_exists()

# Setup worker threads
threads = []
for i in range(NUM_WORKERS):
    t = threading.Thread(target=db_worker, args=(i+1,))
    t.start()
    threads.append(t)

# Start file processing
process_file()

# Wait for all tasks
data_queue.join()
print("[Main] All batches processed.")

# Wait for threads to exit
for t in threads:
    t.join()

print("✅ All data inserted.")
