import pyarrow as pa
import pyarrow.flight as pf

class SimpleFlightServer(pf.FlightServerBase):
    def __init__(self, listen, config, **kwargs):
        super(SimpleFlightServer, self).__init__(listen, **kwargs)
        self._listen = listen
        self._build_flights(config)

    def _build_flights(self, endpoint_config):
        self.endpoints = {}
        for (topic, ec) in endpoint_config.items():
            descriptor = pf.FlightDescriptor.for_path(topic)
            ticket = topic
            endpoints = [pf.FlightEndpoint(ticket, [self._listen])]
            info = pf.FlightInfo(
                ec.schema,
                descriptor,
                endpoints,
                ec.total_records,
                ec.total_bytes)
            self.endpoints[topic] = info

    def list_flights(self, context, criteria):
        return self.endpoints.values()

    def get_flight_info(self, context, descriptor):
        return self.endpoints[descriptor]

    #def do_put(self, context, descriptor, reader, writer):
    #    dataset = descriptor.path[0].decode('utf-8')
    #    dataset_path = self._repo / dataset
    #    # Read the uploaded data and write to Parquet incrementally
    #    with dataset_path.open("wb") as sink:
    #        with pa.parquet.ParquetWriter(sink, reader.schema) as writer:
    #            for chunk in reader:
    #                writer.write_table(pa.Table.from_batches([chunk.data]))

    def list_actions(self, context):
        return []
