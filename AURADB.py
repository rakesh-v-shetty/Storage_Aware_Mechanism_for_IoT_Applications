import threading
import time
import random
from neo4j import GraphDatabase
from datetime import datetime

# --- Sensor Data Uploader ---
class SensorDataUploader:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def upload_data(self, room, sensor_type, value, timestamp):
        with self.driver.session() as session:
            session.execute_write(self._create_or_update_data, room, sensor_type, value, timestamp)

    @staticmethod
    def _create_or_update_data(tx, room, sensor_type, value, timestamp):
        query = (
            "MERGE (r:Room {name: $room}) "
            "MERGE (s:Sensor {type: $sensor_type, room: $room}) "
            "SET s.value = $value, s.timestamp = $timestamp "
            "MERGE (r)-[:HAS_SENSOR]->(s)"
        )
        tx.run(query, room=room, sensor_type=sensor_type, value=value, timestamp=timestamp)

def generate_random_sensor_data():
    return random.randint(20, 30), random.randint(40, 60)

def upload_data_to_neo4j():
    uri = "neo4j+s://ca053e53.databases.neo4j.io"
    user = "neo4j"
    password = "35ceF-cl3NAa0uVIhjy9gu77qw_rRA-_h1qUTQ4JDlo"

    uploader = SensorDataUploader(uri, user, password)
    sensors = ["Temperature", "Humidity"]
    last_house_timestamp = 0
    last_room_timestamp = 0

    try:
        while True:
            timestamp = time.time()
            if timestamp - last_house_timestamp >= 5:
                # Generate identical data for House
                for sensor in sensors:
                    value, _ = generate_random_sensor_data()
                    uploader.upload_data("House", sensor, value, timestamp)
                last_house_timestamp = timestamp

            if timestamp - last_room_timestamp >= 15:
                # Generate different data for Room1 and Room2
                for sensor in sensors:
                    value1, value2 = generate_random_sensor_data()
                    uploader.upload_data("Room1", sensor, value1, timestamp)
                    uploader.upload_data("Room2", sensor, value2, timestamp)
                last_room_timestamp = timestamp
            
            time.sleep(1)  # Short sleep to avoid busy-waiting

    finally:
        uploader.close()

# --- Sensor Data Retriever ---
class SensorDataRetriever:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_sensor_data(self):
        with self.driver.session() as session:
            return session.execute_read(self._fetch_sensor_data)

    @staticmethod
    def _fetch_sensor_data(tx):
        query = """
        MATCH (r:Room)-[:HAS_SENSOR]->(s:Sensor)
        RETURN r.name AS room, s.type AS sensor_type, s.value AS value, s.timestamp AS timestamp
        """
        result = tx.run(query)
        data = []
        for record in result:
            data.append({
                "room": record["room"],
                "sensor_type": record["sensor_type"],
                "value": record["value"],
                "timestamp": datetime.fromtimestamp(record["timestamp"]).strftime('%Y-%m-%d %H:%M:%S')
            })
        return data

def retrieve_data_from_neo4j():
    uri = "neo4j+s://ca053e53.databases.neo4j.io"
    user = "neo4j"
    password = "35ceF-cl3NAa0uVIhjy9gu77qw_rRA-_h1qUTQ4JDlo "

    retriever = SensorDataRetriever(uri, user, password)
    last_print_time = time.time()

    try:
        while True:
            current_time = time.time()
            if current_time - last_print_time >= 15:
                sensor_data = retriever.get_sensor_data()
                house_data = [record for record in sensor_data if record['room'] == 'House']
                room_data = [record for record in sensor_data if record['room'] != 'House']

                print("\n# 1st output (House)")
                house_data.sort(key=lambda x: x['timestamp'])
                for record in house_data:
                    print(f"Room: {record['room']}, Sensor Type: {record['sensor_type']}, Value: {record['value']}, Timestamp: {record['timestamp']}")

                time.sleep(5)  # 5 seconds after 1st output

                print("\n# 2nd output (House) after 5 seconds")
                for record in house_data:
                    print(f"Room: {record['room']}, Sensor Type: {record['sensor_type']}, Value: {record['value']}, Timestamp: {record['timestamp']}")

                time.sleep(5)  # 10 seconds after 1st output

                print("\n# 3rd output (House) after 10 seconds")
                for record in house_data:
                    print(f"Room: {record['room']}, Sensor Type: {record['sensor_type']}, Value: {record['value']}, Timestamp: {record['timestamp']}")

                time.sleep(5)  # 15 seconds before 4th output

                print("\n# 4th output (Room1 and Room2) after 15 seconds")
                for record in room_data:
                    print(f"Room: {record['room']}, Sensor Type: {record['sensor_type']}, Value: {record['value']}, Timestamp: {record['timestamp']}")

                last_print_time = time.time()

    finally:
        retriever.close()

# --- Running Both Functions Concurrently ---
if __name__ == "__main__":
    uploader_thread = threading.Thread(target=upload_data_to_neo4j)
    retriever_thread = threading.Thread(target=retrieve_data_from_neo4j)

    # Start both threads
    uploader_thread.start()
    retriever_thread.start()

    # Wait for both threads to complete
    uploader_thread.join()
    retriever_thread.join()