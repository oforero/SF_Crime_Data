import producer_server

BROKER_URL = "localhost:9092"
def run_kafka_server():
    input_file = "police-department-calls-for-service.json"

    # Create a new produder
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="sf_crime_data.police.service.calls",
        bootstrap_servers=BROKER_URL,
        client_id="sf_data_crime.service.call.producer"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
