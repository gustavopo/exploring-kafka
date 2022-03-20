#Exploring Apache Kafka with Java

###CLI:
- Create kafka topic: 
>kafka-topics --bootstrap-server 127.0.0.1:9092 --create --topic demo_java --partitions 3 --replication-factor 1

- Start kafka console consumer:
>kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic demo_java