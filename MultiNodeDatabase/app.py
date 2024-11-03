import time
import pymysql
from pymysql.cursors import DictCursor
import firebase_admin
from firebase_admin import credentials, db

# Detail koneksi database
host_name = "localhost"
port = 3307
user_name = "root"
password = "maria"
database = "dbbackup"

file_path = 'D:/Python/MultiNodeDatabase/ubikuitas-firebase-adminsdk-px7rs-3013b4d781.json'
cred = credentials.Certificate(file_path)
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://ubikuitas-default-rtdb.firebaseio.com'
})

def get_db_connection():
    return pymysql.connect(
        host=host_name,
        port=port,
        user=user_name,
        password=password,
        database=database,
        cursorclass=DictCursor
    )

def fetch_existing_backup_ids():
    ref = db.reference('customer_backup_data')
    existing_data = ref.get()
    return {item['BackupID'] for item in existing_data.values()} if existing_data else set()

def fetch_and_update_data(existing_backup_ids):
    con = get_db_connection()
    try:
        with con.cursor() as cursor:
            query = "SELECT * FROM customerbackup"
            cursor.execute(query)
            results = cursor.fetchall()
            for row in results:
                backup_id = row['BackupID']
                if backup_id not in existing_backup_ids:
                    print(row)  # Tampilkan di konsol
                    ref = db.reference('customer_backup_data')
                    ref.push(row)  # Mengunggah data baru
                    existing_backup_ids.add(backup_id)  # Tambahkan BackupID ke set
                else:
                    print(f"Data dengan BackupID {backup_id} sudah ada di Firebase. Melewatkan...")
    finally:
        con.close()

def main():
    existing_backup_ids = fetch_existing_backup_ids()  # Ambil BackupID yang sudah ada
    try:
        while True:
            fetch_and_update_data(existing_backup_ids)  # Cek dan unggah data baru
            time.sleep(10)  # Tunggu 10 detik sebelum memeriksa lagi
    except KeyboardInterrupt:
        print("\nProgram dihentikan. Terima kasih!")

if __name__ == '__main__':
    main()
