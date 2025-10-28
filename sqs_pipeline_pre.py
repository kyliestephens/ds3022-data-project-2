import requests
from prefect import flow, task, get_run_logger 
import time
from typing import List, Tuple, Dict, Any
import boto3

@task
def populate_sqs():
    logger = get_run_logger() 
    # URL endpoint that triggers the backend to populate an SQS queue
    url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/uqj5uw"
    #Send a POST request to the API and parse the JSON response
    payload = requests.post(url).json()
    logger.info(f"API Response: {payload}")
    print(payload)
    return payload["sqs_url"]

@task
def process_queue_messages(queue_url: str) -> List[Tuple[int, str]]:
    logger = get_run_logger() 
    """Process all messages from the SQS queue."""
    logger.info("Starting message processing pipeline")
    # Initialize a message processing pipeline for the given SQS queue
    pipeline = SQSMessagePipeline(queue_url)
    #Continuously receive and process messages until the queue is empty
    #batch_size controls how many messages are retrieved per request
    #max_wait_time controls how long to wait (seconds) for messages to appear
    pipeline.process_all_messages(batch_size=10, max_wait_time=5)

    logger.info(f"Total messages processed: {len(pipeline.messages_data)}")

    return pipeline.messages_data

@task
def reassemble_phrase(messages_data: List[Tuple[int, str]]) -> str:
    logger = get_run_logger() 
    #Reassemble the phrase from ordered messages
    #Sort the messages based on their order number (first element in tuple)
    sorted_messages = sorted(messages_data, key=lambda x: x[0])
    #Combine all words into a single string (space-separated)
    phrase = ' '.join([word for _, word in sorted_messages])
    return phrase

@task
def send_solution(uvaid: str, phrase: str, platform: str) -> bool:
    logger = get_run_logger() 
    """Submit the solution to the submission queue."""
    sqs = boto3.client('sqs')
    url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    
    try:
        #Send the phrase and metadata as message attributes to SQS
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
        
        logger.info(f"\n{'='*50}")
        logger.info(f"HTTP Status Code: {response['ResponseMetadata']['HTTPStatusCode']}")
        logger.info(f"Message ID: {response['MessageId']}")
        logger.info(f"{'='*50}\n")

        print(f"\n{'='*50}")
        print(f"Submission Response: {response}")
        print(f"HTTP Status Code: {response['ResponseMetadata']['HTTPStatusCode']}")
        print(f"Message ID: {response['MessageId']}")
        print(f"{'='*50}\n")
        
        #Check for 200 status code
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info("Solution successfully submitted!")
            print("Solution successfully submitted!")
            return True
        else:
            logger.error("Submission failed- unexpected status code")
            print("Submission failed - unexpected status code")
            return False
            
    except Exception as e:
        logger.error(f"Error submitting solution: {e}")
        print(f"Error submitting solution: {e}")
        return False

@flow(name="SQS Message Processor")
def sqs_message_flow(uvaid: str = "uqj5uw"):
    logger = get_run_logger() 
    """Main Prefect flow to process SQS messages and submit solution"""
    #Populate the SQS queue and get the URL
    logger.info("Populating SQS queue")
    queue_url = populate_sqs()
    
    #Process all messages from the queue
    logger.info("Process messages from the queue")
    messages_data = process_queue_messages(queue_url)
    
    #Reassemble the phrase
    logger.info("Reassemble phrases")
    final_phrase = reassemble_phrase(messages_data)
    
    logger.info("="*50)
    logger.info(f"FINAL PHRASE: {final_phrase}")
    logger.info("="*50)
    print(f"\n{'='*50}")
    print(f"FINAL PHRASE: {final_phrase}")
    print(f"{'='*50}\n")

    
    #Submit the solution
    success = send_solution(uvaid, final_phrase, "prefect")
    
    if success:
        print("Pipeline completed successfully!")
        logger.info("Pipeline completed successfully")
    else:
        print("Pipeline completed but submission failed.")
        logger.error("Pipeline completed but submission failed.")
    
    return final_phrase


class SQSMessagePipeline:
    def __init__(self, queue_url: str):
        self.sqs = boto3.client('sqs')
        self.queue_url = queue_url
        self.messages_data: List[Tuple[int, str]] = []
    
    def get_queue_stats(self) -> Dict[str, int]:
        #Get current queue status (number of available, in-flight, delayed messages)
        logger = get_run_logger()
        # Request SQS attributes for queue visibility and message counts
        response = self.sqs.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible',
                'ApproximateNumberOfMessagesDelayed'
            ]
        )
        
        #Parse returned attribute data
        attributes = response['Attributes']
        stats = {
            'available': int(attributes.get('ApproximateNumberOfMessages', 0)),
            'in_flight': int(attributes.get('ApproximateNumberOfMessagesNotVisible', 0)),
            'delayed': int(attributes.get('ApproximateNumberOfMessagesDelayed', 0))
        }
        stats['total'] = stats['available'] + stats['in_flight'] + stats['delayed']
        
        return stats
    
    def receive_and_process_messages(self, max_messages: int = 10, wait_time: int = 5) -> int:
        logger = get_run_logger()
        #Receive messages from queue, parse attributes, and delete them.
        #Returns the number of messages processed.

        try:
            #Request a batch of messages from SQS
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time
            )
            
            #Check if messages were returned
            if 'Messages' not in response:
                logger.info("No messages received from queue.")
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

                #Error Handling    
                except KeyError as e:
                    logger.warning(f"Message missing expected attribute: {e}")
                    print(f"Warning: Message missing expected attribute: {e}")
                    continue
                except Exception as e:
                    logger.error(f"Error retrieving queue stats: {e}")
                    print(f"Error processing message: {e}")
                    continue
            
            logger.info(f"Processed {processed_count} messages in this batch.")
            return processed_count
        
        #Error handling
        except Exception as e:
            logger.error(f"Error receiving messages: {e}")
            print(f"Error receiving messages: {e}")
            return 0
    
    def process_all_messages(self, batch_size: int = 3, max_wait_time: int = 18):
        logger = get_run_logger()

        #Process all messages in the queue until empty.
        #Strategy - Poll continuously until no available messages remain

        logger.info("Starting message processing pipeline")
        print("Starting message processing pipeline...")
        total_processed = 0
        empty_polls = 0
        max_empty_polls = 3  #Stop after 3 consecutive empty polls
        
        while True:
            #Check queue status
            stats = self.get_queue_stats()
            logger.info(
                f"Queue Status - Available: {stats['available']}, "
                f"In-Flight: {stats['in_flight']}, Delayed: {stats['delayed']}, Total: {stats['total']}")
            print(f"\nQueue Status - Available: {stats['available']}, "
                  f"In-Flight: {stats['in_flight']}, Delayed: {stats['delayed']}, "
                  f"Total: {stats['total']}")
            
            #No messages = processing complete
            if stats['total'] == 0:
                logger.info("Queue is empty. Processing complete.")
                print("Queue is empty. Processing complete.")
                break
            
            #If messages are only in-flight or delayed, wait before trying again
            if stats['available'] == 0:
                if stats['in_flight'] > 0 or stats['delayed'] > 0:
                    logger.info("Waiting for messages to become available")
                    print("Waiting for messages to become available...")
                    time.sleep(20)
                    empty_polls = 0  #Reset counter since we know messages exist
                    continue
            
            #Process available messages
            processed = self.receive_and_process_messages(
                max_messages=batch_size,
                wait_time=max_wait_time
            )
            
            if processed > 0:
                total_processed += processed
                logger.info(f"Processed {processed} messages (Total: {total_processed})")
                print(f"Processed {processed} messages (Total: {total_processed})")
                empty_polls = 0
            else:
                empty_polls += 1
                logger.info(f"No messages received (empty poll {empty_polls}/{max_empty_polls})")
                print(f"No messages received (empty poll {empty_polls}/{max_empty_polls})")
                
                if empty_polls >= max_empty_polls:
                    #Double-check queue is actually empty
                    final_stats = self.get_queue_stats()
                    if final_stats['total'] == 0:
                        logger.info("Confirmed: Queue is empty.")
                        print("Confirmed: Queue is empty.")
                        break
                    else:
                        logger.info("Messages still present, continuing...")
                        print("Messages still present, continuing...")
                        empty_polls = 0
                        time.sleep(1)
        
        logger.info("Processing Complete")
        logger.info(f"Total messages processed: {total_processed}")
        logger.info(f"Messages stored: {len(self.messages_data)}")
        print(f"\n=== Processing Complete ===")
        print(f"Total messages processed: {total_processed}")
        print(f"Messages stored: {len(self.messages_data)}")


#Run the flow
if __name__ == "__main__":
    result = sqs_message_flow(uvaid="uqj5uw")