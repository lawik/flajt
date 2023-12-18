import pyarrow as pa
import pyarrow.flight

client = pa.flight.connect("grpc://0.0.0.0:8815")

# Read content of the dataset
for f in client.list_flights():
    #flight = client.get_flight_info(f.descriptor)
    reader = client.do_get(f.endpoints[0].ticket)
    total_rows = 0
    for chunk in reader:
        total_rows += chunk.data.num_rows
    print("Got", total_rows, "rows total")
