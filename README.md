# arrow-kafka-experiments

This is a simple experiment using Arrow and Kafka.

Setup:

1. Kafka producer sending Arrow RecordBatches as messages
    - The data is coming from the nycflights, it randomly picks one row and sends that as a RecordBatch of size 1
    - Sending batches of 1 is not ideal but I try to simulate single events coming from a "real" Kafka event source
    - Another solution would be to have Avro to Arrow converter (https://github.com/ikucan/pykafarr)
2. Kafka consumer reads the RecordBatches
    - Uses DataFusion to make a simple computation, adding the `dep_delay` and the `arr_delay` to create a new Columm: `total_delay`
    - Saves the full batch plus the new column to parquet
    - Saving to a parquet file is similar to what KafkaConnect provides today
3. Use DataFusion (could also use Ballista) to read the Parquet file and do a simple aggregation on the new `total_delay` column

## Running

Start kafka.

```
zookeeper-server-start  ./config/zookeeper.properties
kafka-server-start ./config/server.properties
```

Create one topic

```
kafka-topics --create --topic arrow-test --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```

Run the producer and consumer:

```
python producer.py
```

```
python consumer.py
```

```
python query.py
```

```
   SUM(dep_delay)
0            94.0
```
