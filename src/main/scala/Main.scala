import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SimpleApplication")
      .getOrCreate()

    val wigDataFrame = spark.read
      .format("csv")
      .option("header", true)
      .option("mode", "DROPMALFORMED")
      .load("../ADD_Proj/src/main/resources/wig20_test.csv")

    wigDataFrame.show()
    spark.stop()
  }

}
