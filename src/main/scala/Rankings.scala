import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions._

object Rankings extends App {
  val spark = SparkSession.builder()
    .appName("AverageSalariesPerYear")
    .master("local")
    .getOrCreate()
  val teamsDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/lahman2016?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "lahman2016.teams")
    .option("user", "root")
    .option("password", "1111")
    .load()
  val seasonYearWindow = Window.partitionBy("yearID")
  val rankingsDF = teamsDF.select("teamID", "yearID", "Rank", "AB")
    .withColumn("min_rank", max("Rank").over(seasonYearWindow))
    .withColumn("max_rank", min("Rank").over(seasonYearWindow))
    .where(col("Rank") === col("min_rank") || col("Rank") === col("max_rank"));
  rankingsDF.select(col("teamID").alias("Team ID"), col("yearID").alias("Year"), col("Rank"), col("AB").alias("At Bats")).show()
}
