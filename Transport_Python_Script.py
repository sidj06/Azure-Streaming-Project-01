import random
import time
from azure.servicebus import ServiceBusClient, ServiceBusMessage

# Event Hub configurations
connection_str = '<Connection String from Event Hub >'
event_hub_name = '<Event_Hub_Name>'

# Create a ServiceBusClient
servicebus_client = ServiceBusClient.from_connection_string(conn_str=connection_str, logging_enable=True)

def generate_dummy_transportation_data():
    """Generate dummy transportation data"""
    vehicle_id = random.randint(1000, 9999)
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    gps_coordinates = (random.uniform(-90, 90), random.uniform(-180, 180))
    speed = random.randint(0, 120)
    return {
        "vehicle_id": vehicle_id,
        "timestamp": timestamp,
        "latitude": gps_coordinates[0],
        "longitude": gps_coordinates[1],
        "speed": speed
    }

with servicebus_client:
    sender = servicebus_client.get_queue_sender(queue_name=event_hub_name)
    with sender:
        while True:
            # Generate data and send to Event Hub
            data = generate_dummy_transportation_data()
            message = ServiceBusMessage(str(data))
            sender.send_messages(message)
            print(f"Sent: {data}")
            time.sleep(2)  # Send data every 2 seconds. You can adjust this as needed.

if __name__ == "__main__":
    try:
        print("Starting to send messages...")
        while True:
            pass
    except KeyboardInterrupt:
        print("Stopped.")
