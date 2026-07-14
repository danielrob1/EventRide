import random
import uuid
import json
from datetime import datetime, timedelta
from faker import Faker
from azure.eventhub import EventHubProducerClient, EventData
import logging
from dotenv import load_dotenv
load_dotenv()
import os
from data import generate_ride_confirmation

CONNECTION_STRING = os.getenv("CONNECTION_STRING")
EVENT_HUBNAME = os.getenv("EVENT_HUBNAME")


def send_to_event_hub(ride_data=None, batch_size=1):
    try:
        # Inicializar Event Hub Producer Client
        producer = EventHubProducerClient.from_connection_string(
            CONNECTION_STRING,
            eventhub_name=EVENT_HUBNAME
        )
        # Prepararar registros del viaje
        ride_json = json.dumps(ride_data) 
        # Crear batch
        event_batch = producer.create_batch()
        # Crear el evento con los datos del viaje 
        event = EventData(ride_json)
        # Añadir el evento al batch
        event_batch.add(event)
        # Enviar el batch a Azure Event Hub
        producer.send_batch(event_batch)
        producer.close()
        return "Viaje enviado con exito a Event Hub"
        
    except Exception as e:
        print(f"Error al enviar el viaje a Event Hub: {str(e)}")
        return False



if __name__ == "__main__":
    print("=" * 80)
    print("Confirmación de viaje")
    print("=" * 80)
    ride = generate_ride_confirmation()
    print(json.dumps(ride, indent=2))
    print("\n" + "=" * 80)
    print("Enviando a Event Hub...")
    result = send_to_event_hub(ride)
    print(f"Enviado con éxito a Event Hub: {result}")
    
    