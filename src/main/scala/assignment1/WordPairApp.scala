import org.apache.spark.sql.SparkSession


object WordPairApp {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder
      .appName("Word Pair Count") //orizoume onoma efarmogis WordPairCount


      .config("spark.master", "local[2]")//lrmr sto spark na treksei topika ston driver me 2 purines

      .getOrCreate()

    val sc = spark.sparkContext//create spark context

    // orizoume to path gia to arxeio eisodou pou tha diavasei, einai transformation(lazy) kai den diavazei tpt akoma
    val lines = sc.textFile("Shakespeare.txt")


    // katharizoume to arxeio me sosta grammata simeia stiksis kai kena kai kanei ta grammata ola peza, einai transformation(lazy) kai den ektelei tpt akoma
    val cleaned = lines.map(line =>

      line.replaceAll("[^a-zA-Z ]", "")

        .toLowerCase()
    )

    // Dimiourgia zeugei kleidion-timvn, flatmap einai transformation (lazy)
    val pairs = cleaned.flatMap(line => {

      val words = line.split("\\s+")//xorizei ti grammh se lekseis

      words.sliding(2)//auto eiai parathuro olisthisis wste na diavazei ti diadoxikes leksei kathe fora kai metakineitai mia thesi mprosta kathe fora

        .filter(_.length == 2)
        // kratame mono ta pliri zeugaria kai oxi mia leksi mono

        .map(pair => (pair(0), pair(1)))
      // metatrepoume to zeugari se tuple px ("the","king")

    })

    // Erotima 1
    val pairCounts =
      pairs
        .map(pair => (pair, 1))//einai transformation(lazy) kai arxokopoiei tin timi se 1 tou pair wste na to afxanei kathe fora

        .reduceByKey(_ + _)


    println("Total number of pairs:")

    println(pairCounts.count())// action, edw xekinaei to spark na trexei proti fora kai upologozontai ta transformations pou eixame mexri tora

    // Erotima 2
    val filteredPairs =
      pairs.filter {
        // transformation (lazy)

        case (w1, w2) =>//kanei match tin prwth leksi me ti deuteri
          w1.length >= 3 &&
            w2.length >= 3
        // kratame lekseis mono me 3 xaraktires kai panw

      }

    val filteredCounts =
      filteredPairs
        .map(pair => (pair, 1))
        // transformation (lazy)

        .reduceByKey(_ + _)// transformation (lazy)
        // sto erotima  2 oles oi entoles einai transformation (lazy) kai den exei ektelestei tpt akoma apo to spark


    // Erotima 3
    val top5 =
      filteredCounts
        .map { case (pair, count) => (count, pair) }//kanoume metatropi tou tuple vste na mporesoume na kanoume taksinomisi se fthimousa seira,transformation (lazy)

        .sortByKey(false)
        // false = fthinousa seira

        .take(5)// action , kratame ta 5 prwta


    println("Top 5 pairs:")

    top5.foreach {
      // to foreach einai action kai trexei topika sto pc

      case (count, pair) =>
        println(pair + " -> " + count)
    }

    spark.stop()
  }
}