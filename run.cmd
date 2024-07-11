start bin\start_zookeeper.cmd
timeout 10
start bin\start_brokers.cmd
timeout 10
start bin\create_topics.cmd
timeout 10
start python src\main.py
exit