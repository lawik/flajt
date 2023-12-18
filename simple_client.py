import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.flight

client = pa.flight.connect("grpc://0.0.0.0:8815")

# Read content of the dataset
for f in client.list_flights():
    #flight = client.get_flight_info(f.descriptor)
    total_rows = 0
    all_words = pa.array([], type=pa.string())
    unique_words = pa.array([], type=pa.string())
    while True:
        reader = client.do_get(f.endpoints[0].ticket)
        for chunk in reader:
            total_rows += chunk.data.num_rows
            words_title = pc.utf8_split_whitespace(
                pc.utf8_lower(
                    chunk.data.column("title")
                )
            )
            words_description = pc.utf8_split_whitespace(
                pc.utf8_lower(
                    chunk.data.column("description")
                )
            )
            words = pc.list_flatten(
                pa.concat_arrays(
                    [ words_title
                    , words_description
                    ]
                )
            )
            words = pc.utf8_trim_whitespace(words)

            all_words = pa.concat_arrays([all_words, words])
            unique_words = pc.unique(all_words)
            all_distinct_words = pc.count(all_words)
            print("all distinct words: ", all_distinct_words)
            print("total rows:", total_rows)

        if all_distinct_words.as_py() > 5000:
            break

    value_counts = pc.value_counts(all_words)
    value_counts = value_counts.to_pylist()
    value_counts.sort(key=lambda i: i["counts"], reverse=True)
    for item in value_counts:
        if item["counts"] > 100:
            print(item["counts"], " = ", item["values"])
    #print(value_counts)

    print("Got", total_rows, "rows total")
