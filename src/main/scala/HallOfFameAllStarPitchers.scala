import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

//select * from lahman2016.allstarfull as a inner join (
//SELECT p.playerID ,p.yearID, p.ERA, h.yearID as 'Hall of Fame Induction Year'
// FROM lahman2016.halloffame as h
// inner join lahman2016.pitching as p
// on h.playerID = p.playerID
// where h.inducted = 'Y') as h
//on a.playerID = h.playerID
//and a.yearID = h.yearID
//order by a.playerID;

object HallOfFameAllStarPitchers extends App {
  val spark = SparkSession.builder()
    .appName("HallOfFameAllStarPitchers")
    .master("local")
    .getOrCreate()
  val allStarDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/lahman2016?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "lahman2016.allstarfull")
    .option("user", "root")
    .option("password", "1111")
    .load()
  val hallOfFameDF = spark.read
    .format("jdbc")
    .option("url", "jdbc:mysql://localhost:3306/lahman2016?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .option("dbtable", "lahman2016.halloffame")
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
  val inductedHallOfFameDF = hallOfFameDF
    .where(col("inducted") === 'Y')
    .withColumn("Hall of Fame Induction Year", col("yearid"))
    .select("playerID", "Hall of Fame Induction Year")
  val hallOfFamePitchersDF = inductedHallOfFameDF
    .join(pitchingDF, Seq("playerID"))
    .select("playerID", "yearID", "ERA", "Hall of Fame Induction Year")

  //  val windowByPlayerID = Window.partitionBy("playerID");
  val hallOfFameAllStarPitchersDF = allStarDF.
    select("playerID", "yearID", "gameNum")
    .join(hallOfFamePitchersDF, Seq("playerID", "yearID"))
    .groupBy("playerID", "Hall of Fame Induction Year")
    .agg(avg("ERA").alias("ERA"), count("*").alias("# All Star Appearances"))
    .select("playerID","ERA", "# All Star Appearances","Hall of Fame Induction Year")
    .show()
}
