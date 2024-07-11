:: broker 1
start "Broker 1" cmd /k C:/kafka/kafka_2.12-3.7.1/bin/windows/kafka-server-start.bat C:/kafka/kafka_2.12-3.7.1/config/server.properties
timeout 3
:: broker 2
start "Broker 2" cmd /k C:/kafka/kafka_2.12-3.7.1/bin/windows/kafka-server-start.bat C:/kafka/kafka_2.12-3.7.1/config/server1.properties
timeout 3
:: broker 3
start "Broker 3" cmd /k C:/kafka/kafka_2.12-3.7.1/bin/windows/kafka-server-start.bat C:/kafka/kafka_2.12-3.7.1/config/server2.properties
exit