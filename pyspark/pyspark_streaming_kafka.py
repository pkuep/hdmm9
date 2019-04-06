import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
if __name__ == "__main__":
        sc = SparkContext(appName="PythonStreamingKafkaWordCount")
        ssc = StreamingContext(sc, 10) # 10 second window

        kvs = KafkaUtils.createStream(ssc, "our machine:2181", "raw-event-streaming-consumer",{"my_topic":1})  # set my_topic to a Kafka topic to which you can publis

        lines = kvs.map(lambda x: x[1])
        counts = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
        counts.pprint()

        ssc.start()
        ssc.awaitTermination()

