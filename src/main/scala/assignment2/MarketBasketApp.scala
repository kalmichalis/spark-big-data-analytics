import org.apache.spark.sql.SparkSession

object MarketBasketApp {

  def main(args: Array[String]): Unit = {

    // edw orizoume parametro gia elaxisti emfanisi pou theloume na anazitisoume sta proionta sumfvna me tin ekfonisi
    val s = 2

    val spark = SparkSession.builder
      .appName("Market Basket Analysis")
      .config("spark.master", "local[2]")// 2 purines topika sto pc
      .getOrCreate()

    val sc = spark.sparkContext

    // diavasma tou arxeiou csv
    val lines = sc.textFile("groceries.csv")//lazy


    // kratame ola ta transactions pou exei to csv, lazy
    val transactions = lines.map { line =>
      line
        .split(",")
        .map(_.trim.toLowerCase)
        .filter(_.nonEmpty)
        .toList
    }


    // mono transactions me  >=2 proionta gt theloume apo 2 proionta kai panw ws zeugos,lazy
    val pairs = transactions
      .filter(_.size >= 2)
      .flatMap(_.combinations(2))

    val pairCounts = pairs
      .map(p => (p.sorted, 1))
      .reduceByKey(_ + _)
      .filter { case (_, count) => count >= s }// upologizei na emafinei oti einai apo s timh kai panw pou exoume vw parametro, action logo tou count


    // mono transactions me  >=3 proionta ,lazy

    val triples = transactions
      .filter(_.size >= 3)
      .flatMap(_.combinations(3))

    val tripleCounts = triples
      .map(t => (t.sorted, 1))
      .reduceByKey(_ + _)
      .filter { case (_, count) => count >= s }// upologizei na emafinei oti einai apo s timh kai panw pou exoume vw parametro, action logo tou count


    println("Frequent pairs (>= s):") // edv emfanizontai ta pairs, dhladh an exoume 4 proionta tote emfanizontai me duades anametaksi tous

    pairCounts.collect().foreach {//me to foreach kanoume loop sto pc omws to collect einai epikindino logw tou oti mporei na skasei i mnhmh
      case (items, count) =>// action
        println(items.mkString("(", ", ", ")") + " -> " + count)
    }

    println("\nFrequent triples (>= s):")//edv emfanizontai ta triples , dhladh an exoume 3 proionta tote emfanizontai h 3ada pou katametrithike

    tripleCounts.collect().foreach {
      case (items, count) =>//action
        println(items.mkString("(", ", ", ")") + " -> " + count)
    }

    spark.stop()
  }
}