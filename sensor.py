from neo4j import GraphDatabase
import time

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

def main():
    # Replace with your AuraDB connection details
    uri = "neo4j+s://dba0272b.databases.neo4j.io"
    user = "neo4j"
    password = "Hzyt_US_zvuJofY3BScHGKUgKsHgSjP57Ch53ozi-eE"

    uploader = SensorDataUploader(uri, user, password)

    # Hardcoded sensor data
    data_points = [
        {"rooms": ["Room1", "Room2"], "sensor": "Temperature", "values": [25, 25]},
        {"rooms": ["Room1", "Room2"], "sensor": "Humidity", "values": [50, 48]},
        {"rooms": ["Room1", "Room2"], "sensor": "Temperature", "values": [30, 29]},
        {"rooms": ["Room1", "Room2"], "sensor": "Humidity", "values": [45, 45]},
    ]

    previous_data = {point["sensor"]: None for point in data_points}

    try:
        for point in data_points:
            sensor = point["sensor"]
            data = {room: value for room, value in zip(point["rooms"], point["values"])}
            timestamp = time.time()

            if len(set(data.values())) == 1:
                if previous_data[sensor] != data[point["rooms"][0]]:
                    uploader.upload_data("House", sensor, data[point["rooms"][0]], timestamp)
                    previous_data[sensor] = data[point["rooms"][0]]
            else:
                for room in point["rooms"]:
                    if previous_data[sensor] != data[room]:
                        uploader.upload_data(room, sensor, data[room], timestamp)
                        previous_data[sensor] = data[room]

            time.sleep(1)  # Delay for simulation
    finally:
        uploader.close()

if __name__ == "__main__":
    main()