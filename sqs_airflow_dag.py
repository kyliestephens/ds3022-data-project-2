import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
from typing import List, Tuple, Dict, Any
import boto3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def populate_sqs(**context):
    """Populate SQS queue + return URL"""
    # API endpoint that returns a JSON payload containing an SQS URL
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/uqj5uw"
    payload = requests.post(url).json()
    print(payload)
    
    ## Push the queue URL into Airflow's XCom for downstream tasks to retrieve
    context['ti'].xcom_push(key='queue_url', value=payload["sqs_url"])
    return payload["sqs_url"]

def process_queue_messages(**context):
    """Process all messages from the SQS queue"""
    #Pull queue_url from XCom
    queue_url = context['ti'].xcom_pull(key='queue_url', task_ids='populate_sqs_task')
    
    # Create pipeline instance using the queue URL + process all messages in the queue 
    pipeline = SQSMessagePipeline(queue_url)
    pipeline.process_all_messages(batch_size=10, max_wait_time=5)
    
    #Push messages_data to XCom for next task
    context['ti'].xcom_push(key='messages_data', value=pipeline.messages_data)
    return pipeline.messages_data

def reassemble_phrase(**context):
    """Reassemble the phrase from ordered messages"""
    #Pull messages_data from XCom
    messages_data = context['ti'].xcom_pull(key='messages_data', task_ids='process_messages_task')
    
    #Sort messages by order number to restore the correct sequence + join words together to form final phrase
    sorted_messages = sorted(messages_data, key=lambda x: x[0])
    phrase = ' '.join([word for _, word in sorted_messages])
    
    #Printing final phrase
    print(f"\n{'='*50}")
    print(f"FINAL PHRASE: {phrase}")
    print(f"{'='*50}\n")
    
    #Push phrase to XCom for next task
    context['ti'].xcom_push(key='final_phrase', value=phrase)
    return phrase

def send_solution(**context):
    """Submit the solution to the submission queue"""
    #Pull phrase from XCom
    phrase = context['ti'].xcom_pull(key='final_phrase', task_ids='reassemble_phrase_task')
    
    uvaid = "uqj5uw" 
    platform = "airflow"
    
    sqs = boto3.client('sqs') #creating sqs client
    url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    
    try:
        # Send a message containing the phrase and metadata to the submission queue
        response = sqs.send_message(
            QueueUrl=url,
            MessageBody="Solution submission",
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': uvaid
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )
        
        print(f"\n{'='*50}")
        print(f"Submission Response: {response}")
        print(f"HTTP Status Code: {response['ResponseMetadata']['HTTPStatusCode']}")
        print(f"Message ID: {response['MessageId']}")
        print(f"{'='*50}\n")
        
        #Check for 200 status code
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print("✓ Solution successfully submitted!")
            return True
        else:
            print("✗ Submission failed - unexpected status code")
            return False

    #Error Handling     
    except Exception as e:
        print(f"Error submitting solution: {e}")
        return False


class SQSMessagePipeline:
    """Pipeline class to receive, process, and delete SQS messages"""
    def __init__(self, queue_url: str):
        self.sqs = boto3.client('sqs')
        self.queue_url = queue_url
        self.messages_data: List[Tuple[int, str]] = [] #Holds (order_no, word) tuples
    
    def get_queue_stats(self) -> Dict[str, int]:
        """Get current queue statistics"""
        response = self.sqs.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible',
                'ApproximateNumberOfMessagesDelayed'
            ]
        )
        
        #Parse queue attribute response
        attributes = response['Attributes']
        stats = {
            'available': int(attributes.get('ApproximateNumberOfMessages', 0)),
            'in_flight': int(attributes.get('ApproximateNumberOfMessagesNotVisible', 0)),
            'delayed': int(attributes.get('ApproximateNumberOfMessagesDelayed', 0))
        }
        #Calculate total across all states
        stats['total'] = stats['available'] + stats['in_flight'] + stats['delayed']
        
        return stats
    
    def receive_and_process_messages(self, max_messages: int = 10, wait_time: int = 5) -> int:
        #Receive messages from queue, parse attributes, and delete them
        #Returns the number of messages processed
        try:
            # Request up to `max_messages` from queue with long polling
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time
            )
            
            #Check if messages were returned
            if 'Messages' not in response:
                return 0
            
            messages = response['Messages']
            processed_count = 0
            
            for message in messages:
                #Extract message attributes
                try:
                    order_no = message['MessageAttributes']['order_no']['StringValue']
                    word = message['MessageAttributes']['word']['StringValue']
                    receipt_handle = message['ReceiptHandle']
                    
                    #Convert order_no to integer and store
                    self.messages_data.append((int(order_no), word))
                    
                    #Delete the message from queue
                    self.sqs.delete_message(
                        QueueUrl=self.queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    
                    processed_count += 1
                    
                except KeyError as e:
                    print(f"Warning: Message missing expected attribute: {e}")
                    continue
                except Exception as e:
                    print(f"Error processing message: {e}")
                    continue
            
            return processed_count

        #Error Handling     
        except Exception as e:
            print(f"Error receiving messages: {e}")
            return 0
    
    def process_all_messages(self, batch_size: int = 10, max_wait_time: int = 5):
        #Process all messages in the queue until empty
        #Strategy- Poll continuously until no available messages remain
        print("Starting message processing pipeline")
        total_processed = 0
        empty_polls = 0
        max_empty_polls = 3  #Stop after 3 consecutive empty polls
        
        while True:
            #Check queue status
            stats = self.get_queue_stats()
            print(f"\nQueue Status - Available: {stats['available']}, "
                  f"In-Flight: {stats['in_flight']}, Delayed: {stats['delayed']}, "
                  f"Total: {stats['total']}")
            
            #If no messages- done
            if stats['total'] == 0:
                print("Queue is empty. Processing complete.")
                break
            
            #If messages are only in-flight or delayed, wait before trying again
            if stats['available'] == 0:
                if stats['in_flight'] > 0 or stats['delayed'] > 0:
                    print("Waiting for messages to become available...")
                    time.sleep(20)
                    empty_polls = 0  #Reset counter since we know messages exist
                    continue
            
            #Process available messages
            processed = self.receive_and_process_messages(
                max_messages=batch_size,
                wait_time=max_wait_time
            )
            
            #Update counts and handle empty polls
            if processed > 0:
                total_processed += processed
                print(f"Processed {processed} messages (Total: {total_processed})")
                empty_polls = 0
            else:
                empty_polls += 1
                print(f"No messages received (empty poll {empty_polls}/{max_empty_polls})")
                
                if empty_polls >= max_empty_polls:
                    #Double-check queue is actually empty
                    final_stats = self.get_queue_stats()
                    if final_stats['total'] == 0:
                        print("Confirmed: Queue is empty.")
                        break
                    else:
                        print("Messages still present, continuing...")
                        empty_polls = 0
                        time.sleep(1)
        
        print(f"Processing Complete")
        print(f"Total messages processed: {total_processed}")
        print(f"Messages stored: {len(self.messages_data)}")



with DAG(
    'sqs_message_processor', #Dag ID
    default_args=default_args,
    description='Process SQS messages and submit solution', #Description of DAG
    schedule=None, 
    start_date=datetime(2024, 1, 1), 
    catchup=False,
) as dag:
    
    #Define task sequence with PythonOperators
    populate_sqs_task = PythonOperator(
    task_id='populate_sqs_task',
    python_callable=populate_sqs
)

    process_messages_task = PythonOperator(
    task_id='process_messages_task',
    python_callable=process_queue_messages
)

    reassemble_phrase_task = PythonOperator(
    task_id='reassemble_phrase_task',
    python_callable=reassemble_phrase
)

    send_solution_task = PythonOperator(
    task_id='send_solution_task',
    python_callable=send_solution
)

#Set task dependencies (order in which they will be executed)
populate_sqs_task >> process_messages_task >> reassemble_phrase_task >> send_solution_task

if __name__ == "__main__":
    #Validating the DAG loads successfully 
    print(f"DAG '{dag.dag_id}' loaded successfully")
    print(f"Tasks: {[task.task_id for task in dag.tasks]}")