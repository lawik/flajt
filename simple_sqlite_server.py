import pyarrow as pa
import pyarrow.flight
import adbc_driver_sqlite.dbapi as dbapi
from flajt import SimpleFlightServer

import os

class Config(object):
    def __init__(self, topic, schema, total_records, total_bytes):
        self.topic = topic
        self.schema = schema
        self.total_records = total_records
        self.total_bytes = total_bytes

podcast_schema = pa.schema([
    ("id", pa.int64()),
    ("title", pa.string()),
    ("description", pa.string()),
])
podcast_config = Config("podcasts", podcast_schema, -1, -1)

config = {
    podcast_config.topic: podcast_config
}

class FlightServer(SimpleFlightServer):
    def __init__(self, listen, location, config, **kwargs):
        super(FlightServer, self).__init__(listen, config, **kwargs)
        self._location = location
        self._connection = dbapi.connect(location)
        self.limit = 10000
        self.offset = 0
        # Make_ sample query to derive schema
        result = self._query(0, self.limit)
        self._sample = result

    def _query(self, offset, limit):
        print("fetching offset", offset, "limit", limit)
        with self._connection.cursor() as cur:
            #cur.execute("SELECT * FROM podcasts limit ? offset ?", parameters=(limit, offset))
            cur.execute("SELECT id, title, description FROM podcasts limit ? offset ?", parameters=(limit, offset))
            result = cur.fetch_arrow_table()
            return result

    def do_get(self, context, ticket):
        print("Received get")
        result = self._query(self.offset, self.limit)
        self.offset += self.limit
        print("new offset", self.offset)
        return pa.flight.RecordBatchStream(result)


if __name__ == '__main__':
    path = os.environ["DATABASE_PATH"]
    server = FlightServer("grpc://0.0.0.0:8815", path, config)
    server.serve()
