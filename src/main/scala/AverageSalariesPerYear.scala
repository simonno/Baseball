import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AverageSalariesPerYear  {
  // Main Method
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AverageSalariesPerYear")
      .master("local")
      .getOrCreate()
    val salariesDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/lahman2016?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "lahman2016.salaries")
      .option("user", "root")
      .option("password", "1111")
      .load()
    val fieldingDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/lahman2016?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "lahman2016.fielding")
      .option("user", "root")
      .option("password", "1111")
      .load()
    val pitchingDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/lahman2016?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "lahman2016.pitching")
      .option("user", "root")
      .option("password", "1111")
      .load()
    val pitcherSalariesDF = salariesDF.join(pitchingDF, Seq("playerID", "yearID"))
      .groupBy(col("yearID")).avg("salary").select(col("yearID").alias("Year"), col("avg(salary)").alias("Pitching"))
    val infielderSalariesDF = salariesDF.join(fieldingDF, Seq("playerID", "yearID"))
      .groupBy(col("yearID")).avg("salary").select(col("yearID").alias("Year"), col("avg(salary)").alias("Fielding"))

    val resultDF = pitcherSalariesDF.join(infielderSalariesDF, "Year")
    resultDF.show()
    resultDF.write.option("header", "true").mode("overwrite").format("csv").save("output")
    spark.stop()
  }
}
