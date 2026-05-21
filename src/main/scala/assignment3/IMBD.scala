
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object IMBD {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
          .appName("IMDB Project")
            .master("local[3]")// trexei topika me 3 threads
            .getOrCreate()

    // diavazei to arxeio movies.csv
    val df = spark.read
      .option("header", "true")// to proto row einai headers
      .option("inferSchema", "true")// o Spark katalavainei automata tous typous dedomenwn
      .option("sep", ";")// to CSV xrisimopoiei ; ws diaxwristiko

      .csv("movies.csv") // path tou arxeiou

    // emfanizei to schema debugging gia an dw ta dedomena pws ta exei
    //df.printSchema()
    // deixnei tis 5 prwtes grammes
    //df.show(5)
    //println(df.count()) // emfanizei oles tis tainies

    // metatropi tou imdbRating
    // allazei koma se teleia kai to kanei double gia na mhn exw provlimata stin taksinomisi
    val moviesDF = df
      .withColumn(
        "rating", // neo column rating
        regexp_replace( // antikatastasi charaktirwn
          col("imdbRating"), // apo imdbRating
          ",",               // koma
          "."                // se teleia
        ).cast("double")     // metatropi se double
      )


    // EROTHMA 1
    println("=== ERWTHMA 1 ===")
    // mesos skor kai plithos tainiwn ana etos
    val q1 = moviesDF

      // omadopoihsh ana year
      .groupBy("year")

      // ypologismos mesos oros kai plithos
      .agg(

        // mesos oros rating
        avg("rating")
          .as("average_score"),

        // plithos tainiwn
        count("imdbID")
          .as("movie_count")
      )

      // taksinomisi ana year
      .orderBy("year")

    // emfanisi apotelesmatwn
    q1.show()// ACTION


    // erothma 2

    println("=== ERWTHMA 2 ===")
    // kaliteri tainia ana xwra
    // orismos window ana country
    val windowSpec =
      Window
        // omadopoihsh ana xwra
        .partitionBy("country")
        // taksinomisi me vasi to rating
        .orderBy(
          col("rating").desc
        )

    val q2 = moviesDF
      // prosthiki rank column
      .withColumn(
        "rank",
        // arithmos se kathe omada
        row_number()
          .over(windowSpec)
      )

      // kratame mono tin kaliteri tainia
      .filter(
        col("rank") === 1
      )

      // epilegoume tis sthles pou theloume
      .select(
        "country",
        "title",
        "year",
        "rating"
      )

    // emfanisi
    q2.show()//action


    // EROTHMA 3
    println("=== ERWTHMA 3 ===")
    // krataw mono ti mia eggrafi ana imdbID
    val moviesClean = moviesDF
      .dropDuplicates("imdbID")

    // pairnw ta zeugaria twn tainiwn me diafora rating <= 1
    val moviePairs = moviesClean.as("m1")
      .crossJoin(moviesClean.as("m2"))
      .filter(col("m1.imdbID") <= col("m2.imdbID"))
      .filter(abs(col("m1.rating") - col("m2.rating")) <= 1)
      .select(
        col("m1.imdbID").as("movie1"),
        col("m2.imdbID").as("movie2")
      )

        // emfanisi mono twn 40 prwton grammwn oxi olwn gt kollaei apo to plithos twn tainiwn
    moviePairs.show(40)//ACTION
    println(moviePairs.count())






    /*
    // apothikeusi olwn twn zeugwn se arxeio alla einai p[ara polles kai kollise stin ektelesh tou programmatos
    moviePairs.write
      .mode("overwrite") // an yparxei idi to folder tha to svisei
      .option("header", "true") // prosthetei headers
      .csv("output/movie_pairs")
*/

    spark.stop()

  }
}