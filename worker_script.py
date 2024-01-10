import socket
import threading
# import datetime
# import time
import queue

from apple_health_service import what_sticks_health_service


job_queue = queue.Queue()

# def print_start_and_end_time(user_id,time_stamp_str, add_qty_cat_bool, add_workouts_bool):
def run_what_sticks_health_service(user_id,time_stamp_str, add_qty_cat_bool, add_workouts_bool):
    try:
        # test_func_01(time_stamp_str)
        # db_diagnostics()
        what_sticks_health_service(user_id, time_stamp_str, add_qty_cat_bool, add_workouts_bool)
        # print(f"---- here is the test_string: {test_string}")
    except Exception as e:
        print(f"- its ok we caught the error: {e}")
    # what_sticks_health_service(user_id, time_stamp_str, add_qty_cat_bool, add_workouts_bool)
# def print_start_and_end_time(sleep_time, message):
#     start_time = datetime.datetime.now()
#     formatted_start_time = start_time.strftime("%H:%M:%S")
#     file_name = f"print_start_and_end_time_{start_time.strftime('%H%M%S')}.txt"
    
#     with open(file_name, 'w') as file:
#         file.write(f"Start time: {formatted_start_time}\n")
#         file.write(f"{message}\n")
    
#     time.sleep(int(sleep_time))

#     end_time = datetime.datetime.now().strftime("%H:%M:%S")
#     with open(file_name, 'a') as file:
#         file.write("\n\nEnd time: " + end_time)

def process_job():
    print("*** got here ***")
    while True:
        job_data = job_queue.get()
        if job_data:
            print(f"job_data: {job_data}")
            user_id, time_stamp_str, add_qty_cat_bool, add_workouts_bool = job_data.split(',')
            # sleep_time, message = job_data.split(',')
            # print_start_and_end_time(sleep_time, message)
            # test_string = job_data
            # print_start_and_end_time(test_string)
            # print_start_and_end_time(user_id, time_stamp_str, add_qty_cat_bool, add_workouts_bool)
            run_what_sticks_health_service(user_id, time_stamp_str, add_qty_cat_bool, add_workouts_bool)
            job_queue.task_done()

def handle_client(client_socket):
    message = client_socket.recv(1024).decode('utf-8')
    job_queue.put(message)
    client_socket.sendall("Job added to queue".encode('utf-8'))
    client_socket.close()

def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', 6789))
    server.listen(5)
    print("Worker script is running and waiting for jobs...")

    job_processor_thread = threading.Thread(target=process_job)
    job_processor_thread.daemon = True
    job_processor_thread.start()
    
    while True:
        client_sock, addr = server.accept()
        client_thread = threading.Thread(target=handle_client, args=(client_sock,))
        client_thread.start()

if __name__ == "__main__":
    start_server()