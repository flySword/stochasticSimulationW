import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.control._


object StochasticSimulation {

  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("stochastic simulation").setMaster("local")
    var sc = new SparkContext(conf)
    var testData = sc.textFile("testData.txt")

    // cut the simulation grid to 10*10*10 block, key of blockData is the blocks number,
    // value of blockData is the coordinate and attribute of the point
    var blockData = testData.map(line => {
      var cood = line.split(" ")
      var blockX = cood(0).toInt / 20
      var blockY = cood(1).toInt / 20
      var blockZ = cood(2).toInt / 5

      (blockX + blockY * 10 + blockZ * 100, Array(cood(0).toInt, cood(1).toInt, cood(2).toInt, cood(3).toInt))
    }).groupByKey().partitionBy(new HashPartitioner(2)).persist()
    savePairRdd(blockData,"blockData1")

    for (i <- 0 to 1) {
      for (j <- 0 to 1) {
        for (k <- 0 to 1) {

          // 对已有约束数据中的每个点给定对应的键值，构建新的pairRDD，
          // 使新的pairRDD key为将要计算的blockNum， value为需要的所有点数据
          var simulationBlock = blockData.values.flatMap(coods => {

            var results = ArrayBuffer[Tuple2[Int, Iterable[Array[Int]]]]()
            var iter_coods = coods.iterator
            var cood = iter_coods.next() // 取第一个坐标判断在那个块

            var blockX = cood(0) / 20
            var blockY = cood(1) / 20
            var blockZ = cood(2) / 5

            var tmX: Int = 0
            var tmY: Int = 0
            var tmZ: Int = 0

            for (ii <- (-1) to 1) {
              for (jj <- -1 to 1) {
                for (kk <- -1 to 1) {
                  tmX = blockX + ii
                  tmY = blockY + jj
                  tmZ = blockZ + kk

                  if (tmX >= 0 && tmX < 10 && tmX % 2 == i) {
                    if (tmY >= 0 && tmY < 10 && tmY % 2 == j) {
                      if (tmZ >= 0 && tmZ < 10 && tmZ % 2 == k) {
                        results += Tuple2(tmX + tmY * 10 + tmZ * 100, coods)
                      }
                    }
                  }
                }
              }
            }

            results
          }).reduceByKey(_ ++ _) // 网络传输最耗时

          var simulationRlt = simulationBlock.map(kv => {
            var blockNum = kv._1
            var condDatas = kv._2.toArray

            var blockX = blockNum % 10
            var blockY = (blockNum / 10) % 10
            var blockZ = (blockNum / 100) % 10

            var randomPath = ArrayBuffer[Array[Int]]()
            var minX = blockX * 20
            var minY = blockY * 20
            var minZ = blockZ * 5
            val rand = scala.util.Random
            var xx: Int = 0
            var yy: Int = 0
            var zz: Int = 0
            for (i <- 0 to 9) {
              xx = minX + rand.nextInt(20)
              yy = minY + rand.nextInt(20)
              zz = minZ + rand.nextInt(5)
              randomPath += Array(xx, yy, zz, 0)
            }

            var dist = 0
            var weight = 0.0f
            var totalWeight:Float = 0.0f
            var loop = new Breaks()
            for (i <- randomPath) {
              totalWeight = 0
              loop.breakable {
                for (j <- condDatas) {
                  dist = (j(0) - i(0)) * (j(0) - i(0)) + (j(1) - i(1)) *
                    (j(1) - i(1)) + (j(2) - i(2)) * (j(2) - i(2))

                  if (dist != 0) {
                    weight = 10000.0f/dist
                    totalWeight += weight
                    i(3) +=  (j(3) * weight).toInt //*1000 avoid int 误差
                  } else {
                    i(3) = -9999
                    loop.break
                  }
                }

                i(3) = (i(3)/totalWeight).toInt
              }
            }

            (blockNum, randomPath.filter(a => a(3) != -9999).toIterable)

          })

          blockData = blockData.union(simulationRlt).reduceByKey(new HashPartitioner(2), _ ++ _).persist()
        }
      }
    }

    // result 输出
    blockData.values.map(v => {
      var strBuf = new StringBuilder()
      for (a <- v){
        for (b <- a){
          strBuf.append(b.toString + " ")
        }
        strBuf.append("\n")
      }
      strBuf.toString()
    }).saveAsTextFile("resultic.txt")
  //  blockData.values.saveAsTextFile("result_stachastic.txt")

  }


  def savePairRdd(data:RDD[(Int,Iterable[Array[Int]])],fileName:String): Unit ={
    data.values.map(v => {
      var strBuf = new StringBuilder()
      for (a <- v){
        for (b <- a){
          strBuf.append(b.toString + " ")
        }
        strBuf.append("\n")
      }
      strBuf.toString()
    }).saveAsTextFile(fileName)
  }

  def savePairRddWithKey(data: RDD[(Int, Iterable[Array[Int]])], fileName: String): Unit = {
    data.sortByKey(true,2).map(kv => {
      var blockNum = kv._1
      var blockX = blockNum % 10
      var blockY = (blockNum / 10) % 10
      var blockZ = (blockNum / 100) % 10
      var strBuf = new StringBuilder()

      for (coord <- kv._2) {
        for (b <- coord) {
          strBuf.append(b.toString + " ")
        }
        strBuf.append(blockX + " " + blockY + " " + blockZ)
        strBuf.append("\n")
      }
      strBuf.toString

    }).saveAsTextFile(fileName)
  }
}


