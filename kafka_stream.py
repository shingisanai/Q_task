import requests
import tqdm
import time
import json
import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
#from kafka import KafkaProducer
import pendulum

timezone = pendulum.timezone('UTC')



def get_data(api_key):
    api_key = 'fka_04x4dEPWrjGUcrcle1pr4SrCHskGOZ67B3'  # Replace with your actual API Key
    base_url = 'https://api.followupboss.com/v1/people'
    params = {'limit': 100, 'fields': 'allFields', 'includeTrash': 'true'}
    
    all_people = []
    total_fetched = 0

    try:
        while total_fetched < 100:
            try:
                response = requests.get(base_url, auth=(api_key, ''), params=params)

                if response.status_code == 429:
                    raise Exception("Rate limit hit. Aborting operation.")

                response.raise_for_status()

                data = response.json()
                people = data.get('people', [])
                all_people.extend(people[:max(0, 100 - total_fetched)])
                fetched_this_round = len(people[:max(0, 100 - total_fetched)])
                total_fetched += fetched_this_round
                print(f"Fetched {fetched_this_round} people. Total fetched: {total_fetched}")

                if '_metadata' in data and 'next' in data['_metadata']:
                    params['next'] = data['_metadata']['next']
                else:
                    break

            except requests.RequestException as e:
                print(f"An error occurred: {e}")
                break

    except KeyboardInterrupt:
        print("\nData fetching interrupted by user. Proceeding with the data collected so far.")

    return all_people[:100]

def format_data(all_people):
    data = {}
    data['Id'] = repr(uuid.uuid4())
    data['FUBId'] = all_people['id']
    data['created'] = all_people['created']
    data['updated'] = all_people['updated']
    data['createdVia'] = all_people['createdVia']
    data['name'] = all_people['name']
    data['stage'] = all_people['stage']
    data['stageId'] = all_people['stageId']
    data['source'] = all_people['source']
    data['sourceId'] = all_people['sourceId']
    data['sourceUrl'] = all_people['sourceUrl']
    data['contacted'] = all_people['contacted']
    data['price'] = all_people['price']
    data['assignedUserId'] = all_people['assignedUserId']
    data['assignedPondId'] = all_people['assignedPondId']
    data['assignedTo'] = all_people['assignedTo']
    data['tags']= all_people['tags']
    data['emails'] = all_people['emails']['value']
    data['phones'] = all_people['phones']['value']
    data['addresses'] = all_people['addresses']
    data['websiteVisits'] = all_people['websiteVisits']
    data['claimed'] = all_people['claimed']
    data['lastCommunicationid'] = all_people['lastCommunication']['id']
    data['lastCommunicationType'] = all_people['lastCommunication']['type']
    data['lastCommunicationDate'] = all_people['lastCommunication']['date']
    data['lastCommunicationDirection'] = all_people['lastCommunication']['direction']
    data['lastCommunicationContent'] = all_people['lastCommunication']['content']
    data['AgentId'] = all_people['collaborators']['id']
    data['AgentName'] = all_people['collaborators']['name']
    data['AgentEmail'] = all_people['collaborators']['email']
    data['AgentRole'] = all_people['collaborators']['role']
    
    return data
    
def stream_data():
    api_key = 'fka_04x4dEPWrjGUcrcle1pr4SrCHskGOZ67B3'
    all_people = get_data(api_key)
    all_people = [format_data(person) for person in all_people]
    for person_data in all_people:
        print(json.dumps(person_data, indent=4))

    # Assuming you have Kafka setup
    #producer = KafkaProducer(bootstrap_servers='localhost:9092',max_block_ms=5000)
    #topic = 'your_topic_name'

    #for event in formatted_events:
        #producer.send(topic, json.dumps(event).encode('utf-8'))

    #producer.flush()

#default_args = {
    #'owner': 'Shingi_B',
    #'start_date': datetime(2022, 1, 1, 00, 00)
#}

#with DAG(
    #'FUB_People_Streaming',
    #default_args=default_args,
    #description='DAG for processing and streaming data',
    #schedule_interval='*/3 * * * *',  # Set the schedule interval to every 5 minutes
    #catchup=False
#) as dag:
    
    #streaming_task = PythonOperator(
        #task_id='get_and_stream_data from FUB',
        #python_callable=stream_data,
        #dag=dag,
    #)

#streaming_task