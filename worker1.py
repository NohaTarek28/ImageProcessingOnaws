import boto3
import cv2
import json
import os
import numpy as np
from botocore.exceptions import NoCredentialsError
from mpi4py import MPI
import threading
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


if rank == 0:
    s3_client = boto3.client('s3', region_name='eu-north-1')
    sqs_client = boto3.client('sqs', region_name='us-east-1')
    bucket_name = 'bucketyyimagee'
    queue_url = 'https://sqs.us-east-1.amazonaws.com/654654578707/queue1'
else:
    s3_client = None
    sqs_client = None
    bucket_name = None
    queue_url = None


class WorkerThread:
    def __init__(self):
        self.lock = threading.Lock()
        pass

    def receive_task(self):
        while True:
            with self.lock:
                response = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=10)

            if 'Messages' in response:
                message = response['Messages'][0]
                receipt_handle = message['ReceiptHandle']

                try:
                    task = json.loads(message['Body'])
                except json.JSONDecodeError:
                    print(f"Received an empty or non-JSON message: {message['Body']}")
                    continue

                s3_location = task.get('s3_location')
                operation = task.get('operation')

                if s3_location and operation:
                    with self.lock:
                        sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

                    print(f"Received task with s3_location: {s3_location}, operation: {operation}")
                    return s3_location, operation

            else:
                return None

    def process_image(self, img, operation):
        if operation == 'edgedetection':
            result = cv2.Canny(img, 100, 200)
        elif operation == 'colorinversion':
            result = cv2.bitwise_not(img)
        elif operation == 'blur':
            result = cv2.GaussianBlur(img, (9, 9), 0)
        elif operation == 'erosion':
            se_rect = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
            result = cv2.erode(img, se_rect, iterations=1)
        elif operation == 'dilate':
            se_rect = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
            result = cv2.dilate(img, se_rect, iterations=1)
        else:
            result = img
        return result

    def send_result(self, result):
        message_body = f"s3://{bucket_name}/{result}"
        print(f"Sending result message: {message_body}")
        try:
            with self.lock:
                sqs_client.send_message(QueueUrl=queue_url, MessageBody=message_body)
            print('d5l')
        except NoCredentialsError:
            print("No AWS credentials found")
        except Exception as e:
            print(f"Error occurred: {e}")

    def run(self):
        while True:
            if rank == 0:

                task = self.receive_task()
                if task is None:
                    break
                image, operation = task
                file_name = os.path.basename(image)
                try:
                    s3_client.download_file(bucket_name, file_name, file_name)
                    print(f"Downloaded file: {file_name}")
                except NoCredentialsError:
                    print("No AWS credentials found")
                    return None
                except Exception as e:
                    print(f"Error occurred: {e}")
                    return None
                img = cv2.imread(file_name, cv2.IMREAD_COLOR)
                img_parts = np.array_split(img, size, axis=0)
            else:
                img_parts = None
                operation = None

            operation = comm.bcast(operation, root=0)
            if operation is None:
                break

            img_part = comm.scatter(img_parts, root=0)

            processed_part = self.process_image(img_part, operation)

            processed_parts = comm.gather(processed_part, root=0)

            if rank == 0:
                final_image = np.vstack(processed_parts)
                result_file_name = "result_" + file_name
                print("Uploading file:", result_file_name)
                cv2.imwrite(result_file_name, final_image)
                s3_client.upload_file(result_file_name, bucket_name, result_file_name)
                self.send_result(result_file_name)


if __name__ == "__main__":
    print("Starting worker...")
    worker = WorkerThread()
    worker.run()
