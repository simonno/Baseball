import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Pitching extends App {
  val spark = SparkSession.builder()
    .appName("Pitching")
    .master("local")
    .getOrCreate()
  val pitchingDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/lahman2016?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "lahman2016.pitching")
    .option("user", "root")
    .option("password", "1111")
    .load()
  val pitchingPostDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/lahman2016?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "lahman2016.pitchingpost")
    .option("user", "root")
    .option("password", "1111")
    .load()
  val resultDF = pitchingPostDF
    .withColumn("Regular Season ERA", col("ERA"))
    .withColumn("Regular Season Win/Lose", round(col("w") / (col("w") + col("L")) * 100))
    .select("yearID", "playerID", "Regular Season ERA", "Regular Season Win/Lose")
    .join(
      pitchingDF
        .withColumn("Post-Season ERA", col("ERA"))
        .withColumn("Post-Season Win/Lose", round(col("w") / (col("w") + col("L")) * 100))
        .select("yearID", "playerID", "Post-Season ERA", "Post-Season Win/Lose"),
      Seq("yearID", "playerID"))
  resultDF.show()
  resultDF.write.csv("output/pitching.csv")
  resultDF.write.option("header", "true").mode("overwrite").format("csv").save("output")
  spark.stop()
}
