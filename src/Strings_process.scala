object Strings_process{

  def main(args: Array[String]): Unit = {
    var str = "hello"
    println(str.length)

    //两种for循环,等价
    str.foreach(println)
    for (s <- str) println(s)

    println(str.filter(_ != 'h'))
    println(str.drop(2))
    println(str.take(2))

    var str1 = "aa bb cc,dd ee"
    println(str1.split(","))
    println(str1.split(",").map(_.trim()))

    var aa = 10
    println("aa = %s".format(aa))
    println(s"aa = $aa")
    println(s"aa + 1 = ${aa+1}")
    println(s"aa\nbb")
    println(raw"aa\nbb")

    val pattern = "([0-9]+) ([A-Za-z]+)".r//还有问题
    val pattern(count, fruit) = "100 Bananas"



    var map1 = Map("a"->"1", "b"->"2", "c"->"3")
    println(map1.keys.foreach(println))


  }

}
