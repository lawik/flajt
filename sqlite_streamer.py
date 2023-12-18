import pathlib
import tarfile

import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet
import adbc_driver_sqlite.dbapi as dbapi

import os


class FlightServer(pa.flight.FlightServerBase):
    def __init__(self, location, **kwargs):
        listen = "grpc://0.0.0.0:8815"
        super(FlightServer, self).__init__(listen, **kwargs)
        self._listen = listen
        self._location = location
        self._connection = dbapi.connect(location)
        self.name = "podcasts"
        self.limit = 10000
        self.offset = 0
        # Make_ sample query to derive schema
        result = self._query(0, self.limit)
        #print("fetched 100")
        #print(result.schema)
        self._sample = result
        #print(dir(result))

    def _query(self, offset, limit):
        print("fetching offset", offset, "limit", limit)
        with self._connection.cursor() as cur:
            #cur.execute("SELECT * FROM podcasts limit ? offset ?", parameters=(limit, offset))
            cur.execute("SELECT id, title, description FROM podcasts limit ? offset ?", parameters=(limit, offset))
            result = cur.fetch_arrow_table()
            return result

    def _make_flight_info(self):
        schema = self._sample.schema
        metadata = dict()
        descriptor = pa.flight.FlightDescriptor.for_path("podcasts")
        endpoints = [pa.flight.FlightEndpoint(self.name, [self._listen])]
        return pyarrow.flight.FlightInfo(schema,
                                        descriptor,
                                        endpoints,
                                        -1,
                                        -1)

    def list_flights(self, context, criteria):
        return [self._make_flight_info()]

    def get_flight_info(self, context, descriptor):
        if descriptor == "podcasts":
            return self._make_flight_info()

    #def do_put(self, context, descriptor, reader, writer):
    #    dataset = descriptor.path[0].decode('utf-8')
    #    dataset_path = self._repo / dataset
    #    # Read the uploaded data and write to Parquet incrementally
    #    with dataset_path.open("wb") as sink:
    #        with pa.parquet.ParquetWriter(sink, reader.schema) as writer:
    #            for chunk in reader:
    #                writer.write_table(pa.Table.from_batches([chunk.data]))

    def do_get(self, context, ticket):
        print("Received get")
        result = self._query(self.offset, self.limit)
        self.offset += self.limit
        print("new offset", self.offset)
        #print(result)
        return pa.flight.RecordBatchStream(result)

    def list_actions(self, context):
        return []

    #def do_action(self, context, action):
    #    if action.type == "drop_dataset":
    #        self.do_drop_dataset(action.body.to_pybytes().decode('utf-8'))
    #    else:
    #        raise NotImplementedError

if __name__ == '__main__':

    path = os.environ["DATABASE_PATH"]
    server = FlightServer(location=path)
    server.serve()
