from neo4j import GraphDatabase

class SensorDataRetriever:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_sensor_data(self):
        with self.driver.session() as session:
            session.execute_read(self._fetch_sensor_data)

    @staticmethod
    def _fetch_sensor_data(tx):
        query = """
        MATCH (r:Room)-[:HAS_SENSOR]->(s:Sensor)
        RETURN r.name AS room, s.type AS sensor_type, s.value AS value, s.timestamp AS timestamp
        """
        result = tx.run(query)
        for record in result:
            print(f"Room: {record['room']}, Sensor Type: {record['sensor_type']}, Value: {record['value']}, Timestamp: {record['timestamp']}")

def main():
    # Replace with your AuraDB connection details
    uri = "neo4j+s://dba0272b.databases.neo4j.io"
    user = "neo4j"
    password = "Hzyt_US_zvuJofY3BScHGKUgKsHgSjP57Ch53ozi-eE"

    retriever = SensorDataRetriever(uri, user, password)

    try:
        retriever.get_sensor_data()
    finally:
        retriever.close()

if __name__ == "__main__":
    main()