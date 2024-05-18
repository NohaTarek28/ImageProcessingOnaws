import unittest
from unittest.mock import patch, MagicMock
from moto import mock_aws 
import boto3 
import os
import tkinter as tk
from io import StringIO
import sys


from Master1 import upload_file, download_file, send_message, process_messages, main

class TestImageUploader(unittest.TestCase):

    @mock_aws
    @mock_aws
    def setUp(self):
       
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        self.s3_client.create_bucket(Bucket='imagedumpv2')
        
       
        self.sqs_client = boto3.client('sqs', region_name='us-east-1')
        self.queue_url = self.sqs_client.create_queue(QueueName='Queue1')['QueueUrl']

    def test_upload_file(self):
       
        with open('test_image.jpg', 'w') as f:
            f.write('test image content')
        result = upload_file('test_image.jpg', 'imagedumpv2', 'test_image.jpg')
        self.assertTrue(result)
        os.remove('test_image.jpg')

    def test_download_file(self):
        
        with open('test_image.jpg', 'w') as f:
            f.write('test image content')
        self.s3_client.upload_file('test_image.jpg', 'imagedumpv2', 'test_image.jpg')
        
       
        result = download_file('imagedumpv2', 'test_image.jpg', 'downloaded_image.jpg')
        self.assertTrue(result)
        self.assertTrue(os.path.exists('downloaded_image.jpg'))
        os.remove('test_image.jpg')
        os.remove('downloaded_image.jpg')

    def test_send_message(self):
       
        message_body = 'test message'
        result = send_message(self.queue_url, message_body)
        response = self.sqs_client.receive_message(QueueUrl=self.queue_url)
        self.assertIn('Messages', response)
        self.assertEqual(len(response['Messages']), 1)
        self.assertEqual(response['Messages'][0]['Body'], message_body)

    @patch('Master1.cv2.imread')
    @patch('Master1.cv2.imwrite')
    @patch('Master1.ImageTk.PhotoImage')
    def test_process_messages(self, mock_photo_image, mock_imwrite, mock_imread):
        # Upload a file first and send SQS message
        with open('test_image.jpg', 'w') as f:
            f.write('test image content')
        self.s3_client.upload_file('test_image.jpg', 'imagedumpv2', 'test_image.jpg')
        
        message_body = json.dumps({
            's3_location': 's3://imagedumpv2/test_image.jpg',
            'operation': 'test_operation'
        })
        send_message(self.queue_url, message_body)
     
        root = tk.Tk()
        progress_bar = MagicMock()
        image_label = MagicMock()

      
        process_messages(progress_bar, image_label)
        
    
        progress_bar.__setitem__.assert_called_with('value', 4)
        mock_imread.assert_called()
        mock_imwrite.assert_called()
        mock_photo_image.assert_called()
        os.remove('test_image.jpg')

    def test_main(self):
      
        captured_output = StringIO()
        sys.stdout = captured_output
        
        main()
        
        sys.stdout = sys.__stdout__
        output = captured_output.getvalue()
        self.assertIn('Running script...', output)
        self.assertIn('Starting main function...', output)
