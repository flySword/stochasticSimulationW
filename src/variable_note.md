

- blockData.values.flatMap(cood =>  ArrayBuffer[Tuple2[Int, Array[Int]]]() )

结果类型 org.apache.spark.rdd.RDD[(Int, Array[Int])]

- 调用groupByKey后

结果类型为org.apache.spark.rdd.RDD[(Int, Iterable[Array[Int]])]

- var randomPath = simulationBlock.keys.flatMap(blockNum => ArrayBuffer[Tuple2[Int, Array[Int]]]).groupByKey()

结果类型为org.apache.spark.rdd.RDD[(Int, Iterable[Array[Int]])]

-  var computingRdd = simulationBlock.join(randomPath).groupByKey()
  computingRdd: org.apache.spark.rdd.RDD[(Int, Iterable[(Iterable[Array[Int]], Iterable[Array[Int]])])] 

每次调用groupByKey时，会将key对应的value转为Iterable类型存储

Iterable变量可以调用toArray成为数组





