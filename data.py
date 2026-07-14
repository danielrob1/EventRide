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

fake = Faker()

# Mapeo de tipos de vehículos
VEHICLE_TYPE_MAPPING = [
    {'vehicle_type_id': 1, 'vehicle_type': 'X', 'description': 'Estándar', 'base_rate': 2.50, 'per_mile': 1.75, 'per_minute': 0.35},
    {'vehicle_type_id': 2, 'vehicle_type': 'XL', 'description': 'Extra Grande', 'base_rate': 3.50, 'per_mile': 2.25, 'per_minute': 0.45},
    {'vehicle_type_id': 3, 'vehicle_type': 'Shared', 'description': 'Viaje Compartido', 'base_rate': 2.00, 'per_mile': 1.50, 'per_minute': 0.30},
    {'vehicle_type_id': 4, 'vehicle_type': 'Comfort', 'description': 'Cómodo', 'base_rate': 3.00, 'per_mile': 2.00, 'per_minute': 0.40},
    {'vehicle_type_id': 5, 'vehicle_type': 'Golden', 'description': 'Premium', 'base_rate': 5.00, 'per_mile': 3.50, 'per_minute': 0.60}
]

# Mapeo de métodos de pago
PAYMENT_METHOD_MAPPING = [
    {'payment_method_id': 1, 'payment_method': 'Tarjeta de Crédito', 'is_card': True, 'requires_auth': True},
    {'payment_method_id': 2, 'payment_method': 'Tarjeta de Débito', 'is_card': True, 'requires_auth': True},
    {'payment_method_id': 3, 'payment_method': 'Billetera Digital', 'is_card': False, 'requires_auth': False},
    {'payment_method_id': 4, 'payment_method': 'Efectivo', 'is_card': False, 'requires_auth': False}
]

# Mapeo de estado del viaje
RIDE_STATUS_MAPPING = [
    {'ride_status_id': 1, 'ride_status': 'Completado', 'is_completed': True},
    {'ride_status_id': 2, 'ride_status': 'Cancelado', 'is_completed': False}
]

# Mapeo de marcas de vehículos
VEHICLE_MAKE_MAPPING = [
    {'vehicle_make_id': 1, 'vehicle_make': 'Toyota'},
    {'vehicle_make_id': 2, 'vehicle_make': 'Honda'},
    {'vehicle_make_id': 3, 'vehicle_make': 'Ford'},
    {'vehicle_make_id': 4, 'vehicle_make': 'Chevrolet'},
    {'vehicle_make_id': 5, 'vehicle_make': 'Nissan'},
    {'vehicle_make_id': 6, 'vehicle_make': 'BMW'},
    {'vehicle_make_id': 7, 'vehicle_make': 'Mercedes'}
]

# Listas y diccionarios de mapeo generados a partir de las configuraciones anteriores
VEHICLE_MAKES_LIST = [m['vehicle_make'] for m in VEHICLE_MAKE_MAPPING]
VEHICLE_MAKE_ID_MAP = {m['vehicle_make']: m['vehicle_make_id'] for m in VEHICLE_MAKE_MAPPING}

VEHICLE_TYPES_LIST = [t['vehicle_type'] for t in VEHICLE_TYPE_MAPPING]
VEHICLE_TYPE_ID_MAP = {t['vehicle_type']: t['vehicle_type_id'] for t in VEHICLE_TYPE_MAPPING}

PAYMENT_METHODS_LIST = [p['payment_method'] for p in PAYMENT_METHOD_MAPPING]
PAYMENT_METHOD_ID_MAP = {p['payment_method']: p['payment_method_id'] for p in PAYMENT_METHOD_MAPPING}

RIDE_STATUSES_LIST = [s['ride_status'] for s in RIDE_STATUS_MAPPING]
RIDE_STATUS_ID_MAP = {s['ride_status']: s['ride_status_id'] for s in RIDE_STATUS_MAPPING}

# Mapeo de ciudades
CITY_MAPPING = [
    {'city_id': 1, 'city': 'Madrid', 'state': 'Comunidad de Madrid', 'region': 'Centro'},
    {'city_id': 2, 'city': 'Barcelona', 'state': 'Cataluña', 'region': 'Este'},
    {'city_id': 3, 'city': 'Valencia', 'state': 'Comunidad Valenciana', 'region': 'Este'},
    {'city_id': 4, 'city': 'Sevilla', 'state': 'Andalucía', 'region': 'Sur'},
    {'city_id': 5, 'city': 'Zaragoza', 'state': 'Aragón', 'region': 'Noreste'},
    {'city_id': 6, 'city': 'Málaga', 'state': 'Andalucía', 'region': 'Sur'},
    {'city_id': 7, 'city': 'Murcia', 'state': 'Región de Murcia', 'region': 'Sureste'},
    {'city_id': 8, 'city': 'Palma de Mallorca', 'state': 'Islas Baleares', 'region': 'Islas'},
    {'city_id': 9, 'city': 'Bilbao', 'state': 'País Vasco', 'region': 'Norte'},
    {'city_id': 10, 'city': 'Valladolid', 'state': 'Castilla y León', 'region': 'Noroeste'}
]

CITY_LIST = [c['city'] for c in CITY_MAPPING]
CITY_ID_MAP = {c['city']: c['city_id'] for c in CITY_MAPPING}

# Mapeo de razones de cancelación
CANCELLATION_REASON_MAPPING = [
    {'cancellation_reason_id': 1, 'cancellation_reason': 'Conductor canceló'},
    {'cancellation_reason_id': 2, 'cancellation_reason': 'Pasajero canceló'},
    {'cancellation_reason_id': 3, 'cancellation_reason': 'No se presentó'},
    {'cancellation_reason_id': 4, 'cancellation_reason': None}  # Viajes completados
]

CANCELLATION_REASON_ID_MAP = {c['cancellation_reason']: c['cancellation_reason_id'] for c in CANCELLATION_REASON_MAPPING}


def generate_uber_ride_confirmation():
    
    # Generar marcas de tiempo (timestamps)
    pickup_time = datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))
    duration_minutes = random.randint(5, 120)
    dropoff_time = pickup_time + timedelta(minutes=duration_minutes)
    booking_time = pickup_time - timedelta(minutes=random.randint(1, 10))
    
    # Distancia en millas
    distance = round(random.uniform(0.5, 50), 2)
    
    # Cálculo de tarifas/precios
    base_fare = 2.50
    per_mile_rate = 1.75
    per_minute_rate = 0.35
    surge_multiplier = round(random.uniform(1.0, 2.5), 2)
    
    distance_fare = round(distance * per_mile_rate, 2)
    time_fare = round(duration_minutes * per_minute_rate, 2)
    subtotal = round((distance_fare + time_fare + base_fare) * surge_multiplier, 2)
    tip = round(random.choice([0, 0, 0, 1, 2, 3, 5, random.uniform(1, 20)]), 2)
    total_fare = round(subtotal + tip, 2)
    
    # Detalles de la dirección física (ubicación)
    pickup_address = fake.address().replace('\n', ', ')
    dropoff_address = fake.address().replace('\n', ', ')
    
    # Obtener ciudades y sus IDs correspondientes
    pickup_city = random.choice(CITY_LIST)
    dropoff_city = random.choice(CITY_LIST)
    pickup_city_id = CITY_ID_MAP[pickup_city]
    dropoff_city_id = CITY_ID_MAP[dropoff_city]
    
    # Obtener la marca del vehículo y su ID
    vehicle_make = random.choice(VEHICLE_MAKES_LIST)
    vehicle_make_id = VEHICLE_MAKE_ID_MAP[vehicle_make]
    
    # Determinar el estado de cancelación
    is_cancelled = random.random() < 0.1
    cancellation_reason = None
    cancellation_reason_id = 4  # Por defecto: None (completado)
    if is_cancelled:
        cancellation_reason = random.choice(['Conductor canceló', 'Pasajero canceló', 'No se presentó'])
        cancellation_reason_id = CANCELLATION_REASON_ID_MAP[cancellation_reason]

    # Obtener el tipo de vehículo y su ID
    vehicle_type = random.choice(VEHICLE_TYPES_LIST)
    vehicle_type_id = VEHICLE_TYPE_ID_MAP[vehicle_type]

    # Obtener el método de pago y su ID
    payment_method = random.choice(PAYMENT_METHODS_LIST)
    payment_method_id = PAYMENT_METHOD_ID_MAP[payment_method]

    # Obtener el estado del viaje y su ID
    ride_status = random.choice(['Completado', 'Completado', 'Cancelado'])
    ride_status_id = RIDE_STATUS_ID_MAP[ride_status]
    
    # Estructura de confirmación del viaje
    ride_confirmation = {
        # Identificadores / Claves primarias
        'ride_id': str(uuid.uuid4()),
        'confirmation_number': fake.bothify('??#-####-??##'),
        'passenger_id': str(uuid.uuid4()),
        'driver_id': str(uuid.uuid4()),
        'vehicle_id': str(uuid.uuid4()),
        'pickup_location_id': str(uuid.uuid4()),
        'dropoff_location_id': str(uuid.uuid4()),
        
        # Claves foráneas (FK) a las tablas de mapeo
        'vehicle_type_id': vehicle_type_id,
        'vehicle_make_id': vehicle_make_id,
        'payment_method_id': payment_method_id,
        'ride_status_id': ride_status_id,
        'pickup_city_id': pickup_city_id,
        'dropoff_city_id': dropoff_city_id,
        'cancellation_reason_id': cancellation_reason_id,
        
        # Información del pasajero
        'passenger_name': fake.name(),
        'passenger_email': fake.email(),
        'passenger_phone': fake.phone_number(),
        
        # Información del conductor
        'driver_name': fake.name(),
        'driver_rating': round(random.uniform(4.0, 5.0), 2),
        'driver_phone': fake.phone_number(),
        'driver_license': fake.bothify('??-???-#######'),
        
        # Información del vehículo
        'vehicle_model': fake.word().capitalize(),
        'vehicle_color': random.choice(['Negro', 'Blanco', 'Gris', 'Plateado', 'Azul', 'Rojo']),
        'license_plate': fake.bothify('???-####'),
        
        # Ubicaciones de inicio (pickup) y destino (dropoff)
        'pickup_address': pickup_address,
        'pickup_latitude': round(random.uniform(-90, 90), 6),
        'pickup_longitude': round(random.uniform(-180, 180), 6),
        'dropoff_address': dropoff_address,
        'dropoff_latitude': round(random.uniform(-90, 90), 6),
        'dropoff_longitude': round(random.uniform(-180, 180), 6),
        
        # Detalles del viaje - Métricas / Medidas
        'distance_miles': distance,
        'duration_minutes': duration_minutes,
        'booking_timestamp': booking_time.isoformat(),
        'pickup_timestamp': pickup_time.isoformat(),
        'dropoff_timestamp': dropoff_time.isoformat(),
        
        # Precios y costos - Métricas / Medidas
        'base_fare': base_fare,
        'distance_fare': distance_fare,
        'time_fare': time_fare,
        'surge_multiplier': surge_multiplier,
        'subtotal': subtotal,
        'tip_amount': tip,
        'total_fare': total_fare,
        
        # Calificación y estado
        'rating': random.choice([None, random.randint(1, 5)])
    }
    
    return ride_confirmation