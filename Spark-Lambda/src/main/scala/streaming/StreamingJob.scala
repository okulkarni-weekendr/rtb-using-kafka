package streaming

import domain.{Activity, ActivityByProduct}
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import utils.SparkUtils._

object StreamingJob {
	def main(args: Array[String]): Unit = {
		// setup spark context
		val sc = getSparkContext("Lambda with Spark")
		val sqlContext = getSQLContext(sc)
		import sqlContext.implicits._

		val batchDuration = Seconds(4)

		def streamingApp(sc: SparkContext, batchDuration: Duration) = {
			val ssc = new StreamingContext(sc, batchDuration)

			val inputPath = isIDE match {
				case true => "file:////Users/omkar/workspace/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
				case false => "file:///vagrant/input"
			}

			val textDStream = ssc.textFileStream(inputPath)
			val activityStream = textDStream.transform(input => {
				input.flatMap { line =>
					val record = line.split("\\t")
					val MS_IN_HOUR = 1000 * 60 * 60
					if (record.length == 7)
						Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
					else
						None
				}
			})

			activityStream.transform(rdd => {
				val df = rdd.toDF()
				df.registerTempTable("activity")
				val activityByProduct = sqlContext.sql("""SELECT
                                            product,
                                            timestamp_hour,
                                            sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                            sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                            sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                            from activity
                                            group by product, timestamp_hour """)
				activityByProduct
					.map { r => ((r.getString(0), r.getLong(1)),
						ActivityByProduct(r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4))
					) }
			} ).print()

			ssc
		}

		val ssc = getStreamingContext(streamingApp, sc, batchDuration)
		ssc.start()
		ssc.awaitTermination()

	}

}
