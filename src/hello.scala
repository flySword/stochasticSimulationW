


object HelloWorld {

  /* 这是我的第一个 Scala 程序

   * 以下程序将输出'Hello World!'

   */

  def main(args: Array[String]) {

    var text = ""
    for (i <- -1 to 5){
      var kk = 1
      kk = kk + 1
      println (kk)// 这一点类似java而不是脚本语言

      println("Hello, world!  " + i.toString()) // 输出 Hello World
    }


    for(i <- 0 to 99){
      val rand = scala.util.Random
      var aa = rand.nextInt(100)
      println(if (aa == 99) aa)
    }

    var aa:Int = 10
    println(aa/4)

    // for loop
    import scala.util.control._
    var loop = new Breaks()
    loop.breakable{
      for ( j <- 0 to 5)
      for(i <- 0 to 10){
        if (i == 2)
          loop.break()
        println(i.toString + " " + j.toString)
      }
      println("loop over")
    }
    println("loop.break() to here")


    var l1 = List(1,2,3,4)
    println(l1.toString())

  }

}