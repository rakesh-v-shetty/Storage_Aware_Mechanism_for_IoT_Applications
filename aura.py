from neo4j import GraphDatabase
import time
import random

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

def main():
    # Replace with your AuraDB connection details
    uri = "neo4j+s://f6d5d9e1.databases.neo4j.io"
    user = "neo4j"
    password = "z-9u5bWVGwBnq-fEGv8SA0Bq8ho88-84WSoIb-AziiI"

    uploader = SensorDataUploader(uri, user, password)

    sensors = ["Temperature", "Humidity"]
    previous_data = {sensor: None for sensor in sensors}

    try:
        iteration = 0
        while True:
            for sensor in sensors:
                timestamp = time.time()

                if iteration % 4 != 3:
                    # Generate the same value for both rooms
                    value1, value2 = generate_random_sensor_data()
                    value2 = value1  # Make them the same
                else:
                    # Generate different values for each room
                    value1, value2 = generate_random_sensor_data()

                data = {"Room1": value1, "Room2": value2}

                # Upload data
                if len(set(data.values())) == 1:
                    if previous_data[sensor] != data["Room1"]:
                        uploader.upload_data("House", sensor, data["Room1"], timestamp)
                        previous_data[sensor] = data["Room1"]
                else:
                    for room in data:
                        if previous_data[sensor] != data[room]:
                            uploader.upload_data(room, sensor, data[room], timestamp)
                            previous_data[sensor] = data[room]

                time.sleep(1)  # Delay within the cycle
            
            iteration += 1
            time.sleep(5)  # 5 seconds delay between each data generation cycle

    finally:
        uploader.close()

if __name__ == "__main__":
    main()