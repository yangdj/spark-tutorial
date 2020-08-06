import org.apache.spark.sql.SparkSession

object WordCount {
    def main(args: Array[String]) {
        if (args.length < 2) {
            System.err.println("Usage: SimpleApp <hostname> <port>")
            System.exit(1)
        }

        val host = args(0)
        val port = args(1).toInt

        val spark = SparkSession
            .builder
            .appName("nc stream app")
            .getOrCreate()

        import spark.implicits._

        val lines = spark.readStream
            .format("socket")
            .option("host", host)
            .option("port", port)
            .load()

        val words = lines.as[String].flatMap(_.split(" "))

        val wordCounts = words.groupBy("value").count()

        val query = wordCounts.writeStream
            .outputMode("complete")
            .format("console")
            .start()

        query.awaitTermination()
    }
}