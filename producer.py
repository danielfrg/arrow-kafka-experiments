# %%

import pyarrow as pa

# %%

# my_schema = pa.schema([("field0", pa.int32()), ("field1", t2), ("field2", t4), ("field3", t6)])

data = [
    pa.array([1, 2, 3, 4]),
    pa.array(["foo", "bar", "baz", None]),
    pa.array([True, None, False, True]),
]

# %%

import nycflights13
from nycflights13 import flights

# %%

flights

# %%

table = pa.Table.from_pandas(flights, preserve_index=False)

batches = table.to_batches()
records = batches[0]

# %%

import random
import time

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

# %%

broker = "localhost:9092"
conf = {"bootstrap.servers": broker}

a = AdminClient(conf)

topics = a.list_topics()
print(topics.topics)

p = Producer(conf)
topic = "arrow-test"

# %%

while True:
    row = int(random.random() * (len(flights) - 1))
    print(row)

    batch = records.slice(offset=row, length=1)

    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()

    buf = sink.getvalue()
    print(buf.size)

    p.produce(topic, buf)
    time.sleep(1)

# %%
