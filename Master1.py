import os
import logging
from tkinter import filedialog, ttk, Toplevel
import tkinter as tk
from botocore.client import ClientError
import boto3
import cv2
import numpy as np
import json
import socket
import threading
from PIL import Image, ImageTk
import time
import sys
import io

s3_client = boto3.client('s3', region_name='us-east-1')
sqs_client = boto3.client('sqs', region_name='us-east-1')

# Specify the S3 bucket and SQS queue
bucket_name = 'bucketyyimagee'
queue_url = 'https://sqs.us-east-1.amazonaws.com/654654578707/queue1'

lock = threading.Lock()


class IORedirector(object):
    def __init__(self, text_area):
        self.text_area = text_area


class StdoutRedirector(IORedirector):
    def write(self, str):
        self.text_area.insert(tk.END, str)
        self.text_area.see(tk.END)


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket"""
    if object_name is None:
        object_name = file_name

    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        print(f"Uploaded {file_name} to {bucket}/{object_name}")
    except ClientError as e:
        logging.error(e)
        return False
    return True


def send_message(queue_url, message_body):
    """Send a message to the SQS queue"""
    with lock:
        sqs_client.send_message(QueueUrl=queue_url, MessageBody=message_body)
    print(f"Sent message: {message_body}")
    # sleep for 1 second to allow the message to be processed
    time.sleep(2)


def download_file(bucket, object_name, file_name):
    """Download a file from an S3 bucket"""
    try:
        s3_client.download_file(bucket, object_name, file_name)
        print(f"Downloaded {bucket}/{object_name} to {file_name}")
    except ClientError as e:
        logging.error(e)
        return False
    return True


def process_messages(progress_bar, image_label):
    print("Processing messages...")
    progress_bar['value'] += 1  # Update progress bar
    image_parts = []

    while True:
        with lock:
            response = sqs_client.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=10)
        messages = response.get('Messages', [])
        if messages:
            print(f"Received {len(messages)} message(s)")
            for message in messages:
                print('Received message:', message['Body'])
                progress_bar['value'] += 1  # Update progress bar
                if message['Body'].startswith("s3://"):
                    s3_location = message['Body']
                    bucket, object_name = s3_location.replace("s3://", "").split("/", 1)
                    new_file_name = os.path.join(os.path.expanduser("~"), "Desktop", "processed_" + os.path.basename(object_name))
                    download_file(bucket, object_name, new_file_name)
                    progress_bar['value'] += 1  # Update progress bar
                    img = cv2.imread(new_file_name, cv2.IMREAD_COLOR)
                    file_name = os.path.basename(object_name)
                    final_image_path = os.path.join("C:\\Users\\HP\\Desktop",  f"final_image_{file_name}")
                    cv2.imwrite(final_image_path, img)
                    print(f"Saved the final image as '{final_image_path}'")
                    progress_bar['value'] += 1  # Update progress bar

                    # Display the final image in the GUI
                    image = Image.open(final_image_path)
                    photo = ImageTk.PhotoImage(image)
                    image_label.config(image=photo)
                    image_label.image = photo
                    time.sleep(5)  # Add a sleep between displaying images

                with lock:
                    sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
        else:
            None
            if progress_bar['value'] == 4:
                break


def select_processing_option(root):
    selected_option = tk.StringVar(root)

    def choose_option(option):
        selected_option.set(option)
        print(selected_option.get())
        option_window.destroy()

    option_window = Toplevel(root)
    option_window.title("Select Image Processing Option")

    operations = ["blur", "Erosion", "Dilate","colorinversion", "edgedetection"]
    for op in operations:
        button = tk.Button(option_window, text=op, command=lambda op=op: choose_option(op))
        button.pack(pady=10)

    # Center the option window on the screen
    window_width = 300
    window_height = 250
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    x = (screen_width - window_width) // 2
    y = (screen_height - window_height) // 2
    option_window.geometry(f"{window_width}x{window_height}+{x}+{y}")

    root.wait_window(option_window)

    return selected_option.get()


def upload_image(root, progress_bar, image_label):
    file_path = filedialog.askopenfilename(title="Select an image file")
    if file_path:

        operation = select_processing_option(root)
        print(f"Selected operation: {operation}")

        if operation:  # Only proceed if an operation is selected
            img = cv2.imread(file_path, cv2.IMREAD_COLOR)

            img_file = "whole_image" + os.path.basename(file_path)
            cv2.imwrite(img_file, img)

            upload_file(img_file, bucket_name, img_file)

            message_body = json.dumps({'s3_location': f"s3://{bucket_name}/{img_file}", 'operation': operation})
            send_message(queue_url, message_body)

            # Start processing messages
            msg_processing_thread = threading.Thread(
                target=process_messages, args=(progress_bar, image_label), daemon=True)
            msg_processing_thread.start()


def upload_multiple_images(root, progress_bar, image_label):
    file_paths = filedialog.askopenfilenames(title="Select image files")
    if file_paths:
        operation = select_processing_option(root)
        print(f"Selected operation: {operation}")

        if operation:  # Only proceed if an operation is selected
            for file_path in file_paths:
                img = cv2.imread(file_path, cv2.IMREAD_COLOR)

                img_file = "whole_image" + os.path.basename(file_path)
                cv2.imwrite(img_file, img)

                upload_file(img_file, bucket_name, img_file)

                message_body = json.dumps({'s3_location': f"s3://{bucket_name}/{img_file}", 'operation': operation})
                send_message(queue_url, message_body)

            # Start processing messages
            msg_processing_thread = threading.Thread(
                target=process_messages, args=(progress_bar, image_label), daemon=True)
            msg_processing_thread.start()


def main():
    print("Starting main function...")
    root = tk.Tk()
    root.title("Image Uploader")

    # Center the window on the screen
    window_width = 800
    window_height = 600
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    x = (screen_width - window_width) // 2
    y = (screen_height - window_height) // 2
    root.geometry(f"{window_width}x{window_height}+{x}+{y}")

    # Create a style for the progress bar
    style = ttk.Style()
    style.theme_use('default')
    style.configure("red.Horizontal.TProgressbar", background='red')

    # make button wider
    upload_button = tk.Button(root, text="Upload Image", command=lambda: upload_image(root, progress_bar, image_label), width=20)
    upload_button.pack(pady=20)

    upload_multiple_button = tk.Button(root, text="Upload Multiple Images", command=lambda: upload_multiple_images(
        root, progress_bar, image_label), width=20)
    upload_multiple_button.pack(pady=20)

    # Create the progress bar
    # add label to show progress
    progress_label = tk.Label(root, text="Progress:", font="default 12 bold")
    progress_label.pack()
    progress_bar = ttk.Progressbar(
        root, length=500, mode='determinate', maximum=3, style="red.Horizontal.TProgressbar")
    progress_bar.pack(pady=20)

    # Create a label to display the final image
    image_label = tk.Label(root)
    image_label.pack(pady=20)

    # Create a text widget for logging
    Monitored = tk.Label(root, text="Monitoring Logs:", font="default 12 bold")
    Monitored.pack()
    log_frame = tk.Frame(root)
    log_frame.pack(pady=20)

    log_text = tk.Text(log_frame, width=60, height=15)
    log_text.pack(side='left', fill='y')

    scrollbar = tk.Scrollbar(log_frame, command=log_text.yview)
    scrollbar.pack(side='right', fill='y')

    log_text['yscrollcommand'] = scrollbar.set

    # Redirect standard output to the text widget
    sys.stdout = StdoutRedirector(log_text)

    root.mainloop()


if __name__ == "__main__":
    print("Running script...")
    main()
