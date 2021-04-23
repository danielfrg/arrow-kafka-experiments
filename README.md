# arrow-kafka-flight

Start kafka.

```
zookeeper-server-start  ./config/zookeeper.properties
kafka-server-start ./config/server.properties
```

Create one topic

```
kafka-topics --create --topic arrow-test --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```
