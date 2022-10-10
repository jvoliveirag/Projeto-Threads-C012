"""
Main module containing all the code.
"""
from pymongo import MongoClient
from pymongo.errors import PyMongoError

import threading
import time
from contextlib import contextmanager
from random import randint
from typing import Dict, Union


JSON = Dict[str, Union[str, int]]


@contextmanager
def connect_to_mongodb():
    """
    Function to connect to the mongodb database.
    This function should be used as a context manager.
    """
    try:
        client = MongoClient('mongodb://localhost:27017')

        db = client['projetoC012']

        yield db

    except PyMongoError as error:
        print('ERROR WHILE TRYING TO CONNECT TO THE DATABASE')
        print(error)


def process_sensors() -> None:
    """
    Function to create threads to process all sensors.
    """
    sensor_1_thread = threading.Thread(target=process_sensor, args=('Quintal',))
    sensor_2_thread = threading.Thread(target=process_sensor, args=('Quarto',))
    sensor_3_thread = threading.Thread(target=process_sensor, args=('Sala',))
    sensor_4_thread = threading.Thread(target=process_sensor, args=('Cozinha',))

    sensor_1_thread.start()
    sensor_2_thread.start()
    sensor_3_thread.start()
    sensor_4_thread.start()


def process_sensor(sensor_name: str):
    """
    Function to process a single sensor.

    Parameters
    -----------
    sensor_name : str
        The name of the sensor
    """
    while True:
        is_sensor_alarm_triggered = check_sensor_alarm(sensor_name)

        if is_sensor_alarm_triggered:
            show_alarm_message(sensor_name)

        else:
            read_new_sensor_temperature(sensor_name)

        time.sleep(2)


def check_sensor_alarm(sensor_name: str) -> bool:
    """
    Function to check if the sensor alarm is triggered.

    Parameters
    -----------
    sensor_name : str
        The name of the sensor

    Returns
    --------
    bool
        - True if the alarm is triggered;
        - False otherwise.
    """
    db_filter_json = {
        "nomeSensor": sensor_name
    }

    with connect_to_mongodb() as db:
        res = db.sensores.find_one(db_filter_json)

    if not res:
        return False

    if res['sensorAlarmado'] == False:
        return False

    return True


def show_alarm_message(sensor_name: str) -> None:
    """
    Function to show some alarm message.

    Parameters
    -----------
    sensor_name : str
        The name of the sensor
    """
    print(f'Atenção! Temperatura muito alta! Verificar Sensor {sensor_name}!')


def read_new_sensor_temperature(sensor_name: str):
    """
    Function to read a new sensor temperature.

    Parameters
    -----------
    sensor_name : str
        The name of the sensor
    """
    sleep_time = randint(1, 3)
    time.sleep(sleep_time)

    print(f'PROCESSANDO SENSOR: {sensor_name}')

    sensor_temperature = get_sensor_temperature()

    sensor_payload = {
        'sensor_name': sensor_name,
        'sensor_value': sensor_temperature
    }

    store_temperature(sensor_payload)


def get_sensor_temperature() -> int:
    """
    Function to get the value of a a sensor IoT device.

    Returns
    -------
    int
        The sensor temperature.
    """
    temperature = generate_random_temperature()
    return temperature


def generate_random_temperature() -> int:
    """
    Function to generate a random temperature from 30º to 40º C.

    Returns
    -------
    int
        The random temperature.
    """
    return randint(30, 40)


def store_temperature(sensor_data: JSON) -> None:
    """
    Function process and store the temperature in a database.

    Parameters
    ----------
    sensor_data : JSON
        All the data related to the sensor.
    """
    sensor_payload = generate_sensor_payload(sensor_data)

    is_sensor_in_database = check_if_sensor_in_database(sensor_payload)

    if is_sensor_in_database:
        update_sensor_value(sensor_payload)

    else:
        insert_sensor_into_database(sensor_payload)


def generate_sensor_payload(sensor_data: JSON) -> JSON:
    """
    Function to generate the sensor payload that will be stored in the database.

    Parameters
    -----------
    sensor_data : JSON
        The data from the sensor.

    Returns
    --------
    JSON
        The sensor payload already processed.
    """
    sensor_name = sensor_data['sensor_name']
    sensor_value = sensor_data['sensor_value']

    sensor_payload = {
        'nomeSensor': sensor_name,
        'valorSensor': sensor_value,
        'sensorAlarmado': False,
        'unidadeMedida': 'ºC'
    }

    if sensor_value > 38:
        sensor_payload['sensorAlarmado'] = True

    return sensor_payload


def check_if_sensor_in_database(sensor_payload: JSON) -> bool:
    """
    Function to check if the sensor is already in the database.

    Parameters
    -----------
    sensor_payload : JSON
        The sensor json to check if its already in the database.

    Returns
    --------
    bool
        - True if the sensor is already in the database;
        - False otherwise.
    """
    db_filter_json = {
        "nomeSensor": sensor_payload['nomeSensor']
    }

    with connect_to_mongodb() as db:
        res = db.sensores.find_one(db_filter_json)


    if res:
        return True

    return False


def update_sensor_value(sensor_payload: JSON):
    """
    Function to update the sensor value.

    Parameters
    -----------
    sensor_payload : JSON
        The sensor json to update in the database.

    """
    sensor_name = sensor_payload['nomeSensor']
    sensor_value = sensor_payload['valorSensor']
    is_sensor_alarmed = sensor_payload['sensorAlarmado']

    db_filter_json = {
        "nomeSensor": sensor_name
    }

    db_update_json = {
        "$set": {
            'valorSensor': sensor_value,
            'sensorAlarmado': is_sensor_alarmed
        }
    }

    with connect_to_mongodb() as db:
        res = db.sensores.update_one(
            db_filter_json,
            db_update_json
        )

    if res:
        print(f"Sensor: {sensor_name} atualizado!")
    
    else:
        print(f'Erro ao atualizar o sensor {sensor_name}')


def insert_sensor_into_database(sensor_payload: JSON) -> None:
    """
    Function to insert the data received as argument in the database.

    Parameters
    -----------
    sensor_payload : JSON
        The json to be stored in the database.
    """
    with connect_to_mongodb() as db:
        res = db.sensores.insert_one(sensor_payload)

    if res:
        print('Sensor adicionado no banco!')

    else:
        print('Erro ao adicionar o sensor no banco...')


def main():
    """
    Main function. This is where the application starts.
    """
    print('INICIANDO O PROCESSAMENTO DOS SENSORES...')

    process_sensors()


if __name__ == '__main__':
    main()