package batch

import config.Settings
import org.apache.spark.sql.SaveMode
import utils.SparkUtils._

object BatchJob {
	def main (args: Array[String]): Unit = {

		// setup spark context
		val sc = getSparkContext("Lambda with Spark")
		val sqlContext = getSQLContext(sc)
		val wlc = Settings.WebLogGen

		val inputDF = sqlContext.read.parquet(wlc.hdfsPath)
	    	.where("unix_timestamp() - timestamp_hour / 1000 <= 60 * 60 * 6")

		inputDF.registerTempTable("activity")

		val visitorsByProduct = sqlContext.sql(
			"""SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
			  |FROM activity GROUP BY product, timestamp_hour
			""".stripMargin)

		val activityByProduct = sqlContext.sql("""SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_hour """).cache()

		activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs://lambda-pluralsight:9000/lambda/batch1")

		visitorsByProduct.foreach(println)
		activityByProduct.foreach(println)

	}
}
