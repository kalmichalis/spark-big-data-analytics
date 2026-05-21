import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Covid {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Vaccination Analysis")   // onoma efarmogis
      .master("local[3]")                // trexei lokal me 3 purines topika sto pc
      .getOrCreate()


    // fortonoume to dataset apo csv, ekana metatropi tou csv.xlsx se scv wste na mporei to programma na to diavasei
    val df = spark.read
      .option("header", "true")         // exei header
      .option("inferSchema", "true")    // katalavainei  automata tous typous dedomenwn
      .option("sep", ";")               // o separator einai ;
      .csv("country_vaccinations_by_manufacturer.csv")


    // metatrepoume to date apo string se date format gia na kanoume analyseis
    val df2 = df.withColumn(
      "date_parsed",
      to_date(col("date"), "d/M/yyyy")
    )

    // kratame mono tis egkyres hmerominies diladi oti den einai null
    val cleanDF = df2.filter(col("date_parsed").isNotNull)


    val windowSpec = Window
      .partitionBy("location", "vaccine")  // xwrizoume ana xwra kai emvolio
      .orderBy("date_parsed")              // taxinomoume xronologika

    // ftiaxnei nea stili pou exei tin proigoumeni athroistiki timi emvoliasmwn
    // gia kathe location me emvolio me vasi tin hmerominia
    val df3 = cleanDF.withColumn("prev_total",lag("total_vaccinations", 1).over(windowSpec))

      // ypologismos hmeriswn emvoliasmwn
      // (current total - previous total)
      .withColumn("daily_vaccinations",when(col("prev_total").isNull, 0) // prwti eggrafi den exei proigoumeni timi
          .otherwise(col("total_vaccinations") - col("prev_total")))


    // EROTIMA 1

    // SUNOLIKA
    val avg_all = df3.agg(avg("daily_vaccinations").as("avg_daily_all")).show()
    // mono 2021
    val avg_2021 = df3
      .filter(year(col("date_parsed")) === 2021)
      .agg(avg("daily_vaccinations").as("avg_daily_2021")).show()
    // mono 2022
    val avg_2022 = df3
      .filter(year(col("date_parsed")) === 2022)
      .agg(avg("daily_vaccinations").as("avg_daily_2022")).show()






    // EROTHMA 2

    // ypologizoume global average
    val globalAvg = df3.agg(avg("daily_vaccinations")).first().getDouble(0)

    val daysAbove = df3
      // kratame mono meres me timh > average
      .filter(col("daily_vaccinations") > globalAvg)

      // metrame poses fores symvainei ana xwra
      .groupBy("location")
      .count()
      .withColumnRenamed("count", "days_above_global_avg")

      // taxinomisi fthinousa
      .orderBy(desc("days_above_global_avg"))

    daysAbove.show(40)// EMFANIZW MONO TA 40 PRWTA


    // EROTHMA 3
    // synolo xrisis emvoliou ana xwra kai emvolio
    val vaccinesAgg = df3
      .groupBy("location", "vaccine")
      .agg(max("total_vaccinations").as("total_used"))

    // pio xrisimopoiimeno emvolio
    println("===== PERISSOTERO XRHSIMOPOIOUMENO EMVOLIO =====")
    val wMost = Window
      .partitionBy("location")
      .orderBy(desc("total_used"))

    val mostUsed = vaccinesAgg
      .withColumn("rank",
        row_number().over(wMost))
      .filter(col("rank") === 1)//krataei mono tin 1i thesi (rank = 1) mesa se kathe xwra alliws tha emfanisei ola ta emvolia tis kathe xwras

    mostUsed.show()

    // ligotero xrisimopoiimeno
    println("===== LIGOTERO XRHSIMOPOIOUMENO EMVOLIO =====")
    val wLeast = Window
      .partitionBy("location")
      .orderBy("total_used")

    val leastUsed = vaccinesAgg
      .withColumn("rank",
        row_number().over(wLeast))
      .filter(col("rank") === 1)//krataei mono tin 1i thesi (rank = 1) mesa se kathe xwra alliws tha emfanisei ola ta emvolia tis kathe xwras

    leastUsed.show()


    spark.stop()
  }
}