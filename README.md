# 🗃️ NDJSON to MySQL Data Loader with Multithreading

This project is designed to **efficiently import large NDJSON files into a MySQL database** using Python. It uses **multithreading and batch processing** to maximize performance and system resource usage. This is especially useful for large-scale enterprise applications where speed, reliability, and fault tolerance are crucial.

---

## 📂 Project Structure

| File Name            | Description                                                                 |
|----------------------|-----------------------------------------------------------------------------|
| `main.py`            | Main Python script to handle file reading, JSON parsing, and DB insertion.  |
| `sample_ndjson.txt`  | A sample NDJSON (Newline Delimited JSON) file for testing.                  |
---
## 🔧 Technology Stack

- Python 3
- MySQL Database
- `mysql-connector-python` for DB communication
- Python `threading` and `queue` modules for concurrency
---
## 📌 Features Implemented

- Multithreaded data insertion using `Thread` and `Queue`
- Auto-creation of database table if not present
- Efficient batch insertions
- Error handling for file parsing and DB issues
- Logs for thread activity and insert confirmation
- Graceful shutdown of worker threads
---
## 📜 Codebase Walkthrough

### ✅ 1. **Import Statements**

``python
import json
import mysql.connector
import threading
from queue import Queue

json: Parses each NDJSON line.
mysql.connector: Connects and inserts data into MySQL.
threading: Used to create worker threads.
Queue: Used to safely share data (batches) between threads.

 ### ✅2. Configuration Parameters
BATCH_SIZE = 1000
NUM_WORKERS = 2
FILE_PATH = 'C:/Users/vatsa/Documents/Python/ndjson_mysql_project/sample_ndjson.txt'

BATCH_SIZE: Controls how many records are inserted at once.
NUM_WORKERS: Number of concurrent threads to handle DB insertion.
FILE_PATH: Location of the .ndjson file to read.

db_config = {
    "host": "localhost",
    "user": "root",
    "password": "vatsal2208",
    "database": "ndjsondb"
}
### ✅3. Thread-safe Queue Initialization
data_queue = Queue(maxsize=10)

### ✅4. Worker Thread Function
def db_worker(worker_id=0):
    ...

Each thread connects to MySQL and waits for data in data_queue.
When a batch arrives, it performs an SQL INSERT INTO operation.
If None is received, the thread exits (this is the "poison pill").

 ** Key steps inside the worker:

Connects to DB
Waits for batch
Executes batch insert using executemany()
Logs insert or error
Closes connection when done
    
### ✅5. Ensure Table Exists
def ensure_table_exists():
    ...
    
CREATE TABLE IF NOT EXISTS `table` (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    age INT
)

### ✅6. Main File Processing Logic
def process_file():
    ...
** What it does:

Opens the .ndjson file
Reads line by line
Parses JSON safely
Extracts name and age
Appends to current batch
Once BATCH_SIZE is reached, batch is pushed to the queue
At the end, remaining records (if any) are also pushed
Sends None to each worker thread to signal shutdown

** It also handles:

File read errors
JSON parse errors
Logs line number on failure

 
 ✅OUTPUT :

![image](https://github.com/user-attachments/assets/dd4355ad-08a5-4907-8028-61e6bc13c127)
![image](https://github.com/user-attachments/assets/8feb4895-8c74-4799-aea3-71e8792e620c)



![image](https://github.com/user-attachments/assets/12838b56-6061-470a-b99f-733127c6cc53)
