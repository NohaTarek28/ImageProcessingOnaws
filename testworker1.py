import unittest
from unittest.mock import patch, MagicMock
from moto import mock_aws
import boto3
import os
import json
import numpy as np
import cv2
from worker1 import WorkerThread

class TestWorkerThread(unittest.TestCase):

    @mock_aws
    @mock_aws
    def setUp(self):
        # Set up mock S3
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        self.s3_client.create_bucket(Bucket='bucketyyimagee')

        # Set up mock SQS
        self.sqs_client = boto3.client('sqs', region_name='us-east-1')
        self.queue_url = self.sqs_client.create_queue(QueueName='Queue1')['https://sqs.us-east-1.amazonaws.com/654654578707/queue1']

        # Set up the WorkerThread
        self.worker = WorkerThread()

    @patch('worker1.sqs_client', new_callable=lambda: boto3.client('sqs', region_name='us-east-1'))
    @patch('worker1.s3_client', new_callable=lambda: boto3.client('s3', region_name='us-east-1'))
    def test_receive_task(self, mock_sqs_client, mock_s3_client):
        task_message = {
            's3_location': 's3://bucketyyimagee/test_image.jpg',
            'operation': 'edgedetection'
        }
        mock_sqs_client.receive_message.return_value = {
            'Messages': [{
                'Body': json.dumps(task_message),
                'ReceiptHandle': 'fake-receipt-handle'
            }]
        }

        result = self.worker.receive_task()
        self.assertIsNotNone(result)
        self.assertEqual(result, ('s3://bucketyyimagee/test_image.jpg', 'edgedetection'))

    @patch('worker1.cv2.imread', return_value=np.zeros((100, 100, 3), dtype=np.uint8))
    @patch('worker1.s3_client', new_callable=lambda: boto3.client('s3', region_name='us-east-1'))
    def test_process_image(self, mock_s3_client, mock_cv2_imread):
        img = np.zeros((100, 100, 3), dtype=np.uint8)
        operation = 'edgedetection'

        result = self.worker.process_image(img, operation)
        if operation == 'edgedetection':
            # cv2.Canny produces a single-channel image
            self.assertEqual(result.shape, (100, 100))
        else:
            # Other operations preserve the original shape
            self.assertEqual(result.shape, img.shape)

    @patch('worker1.s3_client', new_callable=lambda: boto3.client('s3', region_name='us-east-1'))
    @patch('worker1.sqs_client', new_callable=lambda: boto3.client('sqs', region_name='us-east-1'))
    def test_send_result(self, mock_sqs_client, mock_s3_client):
        result = 'result_test_image.jpg'
        mock_sqs_client.send_message.return_value = {'MessageId': 'fake-message-id'}

        self.worker.send_result(result)
        mock_sqs_client.send_message.assert_called_once()

    @patch('worker1.cv2.imread', return_value=np.zeros((100, 100, 3), dtype=np.uint8))
    @patch('worker1.cv2.imwrite', return_value=True)
    @patch('worker1.s3_client', new_callable=lambda: boto3.client('s3', region_name='us-east-1'))
    @patch('worker1.sqs_client', new_callable=lambda: boto3.client('sqs', region_name='us-east-1'))
    @patch('worker1.comm', autospec=True)
    def test_run(self, mock_comm, mock_sqs_client, mock_s3_client, mock_cv2_imwrite, mock_cv2_imread):
        task_message = {
            's3_location': 's3://bucketyyimagee/test_image.jpg',
            'operation': 'edgedetection'
        }
        mock_sqs_client.receive_message.return_value = {
            'Messages': [{
                'Body': json.dumps(task_message),
                'ReceiptHandle': 'fake-receipt-handle'
            }]
        }
        mock_comm.Get_rank.return_value = 0
        mock_comm.Get_size.return_value = 1
        mock_comm.bcast.return_value = 'edgedetection'
        mock_comm.scatter.return_value = np.zeros((100, 100, 3), dtype=np.uint8)
        mock_comm.gather.return_value = [np.zeros((100, 100, 3), dtype=np.uint8)]

        self.worker.run()
        mock_cv2_imread.assert_called_once()
        mock_cv2_imwrite.assert_called_once()
        mock_s3_client.upload_file.assert_called_once()

if __name__ == '__main__':
    unittest.main()
