# %%

import logging
import random
import time

import datafusion
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaException

F = datafusion.functions


# %%

broker = "localhost:9092"
group = "my-group2"
topics = ["arrow-test"]


conf = {
    "bootstrap.servers": broker,
    "group.id": group,
    "session.timeout.ms": 6000,
    # "auto.offset.reset": "latest",  # default
    "auto.offset.reset": "earliest",
}

# Create logger for consumer (logs will be emitted when poll() is called)
logger = logging.getLogger("consumer")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)-15s %(levelname)-8s %(message)s"))
logger.addHandler(handler)


# Create Consumer instance
# Hint: try debug='fetch' to generate some log messages
c = Consumer(conf, logger=logger)


def print_assignment(consumer, partitions):
    print("Assignment:", partitions)


# Subscribe to topics
c.subscribe(topics, on_assign=print_assignment)

ctx = datafusion.ExecutionContext()

# %%

try:
    i = 0
    while True:
        msg = c.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            # Read message
            buf = msg.value()
            reader = pa.ipc.open_stream(buf)

            batches = [b for b in reader]
            batch = batches[0]
            # print(batches[0])
            # print(pa.Table.from_batches(batches).to_pandas())

            # Create a RecordBatch just with int for datafusion (string not supported)
            comp_batch = pa.RecordBatch.from_arrays(
                [batch["dep_delay"], batch["arr_delay"]],
                names=["dep_delay", "arr_delay"],
            )

            # Create the datafusion DF
            df = ctx.create_dataframe([[comp_batch]])

            # Do simple computation
            df = df.select(
                F.col("dep_delay") + F.col("arr_delay"),
            )

            computed_batch = df.collect()

            # Create table from read data
            table = pa.Table.from_batches(batches)

            # Append new column
            table = table.append_column("total_delay", computed_batch[0].column(0))

            print(table.to_pandas())

            # Create writter
            if i == 0:
                pqwriter = pq.ParquetWriter("./data/sample.parquet", table.schema)
                i = 1

            # Append to Parquet
            pqwriter.write_table(table)

            import time

            time.sleep(1)

except KeyboardInterrupt:
    print("%% Aborted by user\n")

finally:
    if pqwriter:
        pqwriter.close()


# %%
