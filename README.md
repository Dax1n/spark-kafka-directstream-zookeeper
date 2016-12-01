# spark-kafka-directstream-zookeeper
# spark streaming 结合kafka direct创建Dstream方式，偏移是由spark自己存储，存储在checkpoint中，但是这样影响kafka监控工具的使用，所以需要手动实现更新偏移到zookeeper集群
