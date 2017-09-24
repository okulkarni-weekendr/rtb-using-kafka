package streaming

import com.twitter.algebird.HyperLogLogMonoid
import domain.{Activity, ActivityByProduct, VisitorsByProduct}
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import utils.SparkUtils._
import functions._

object StreamingJob {
	def main(args: Array[String]): Unit = {
		// setup spark context
		val sc = getSparkContext("Lambda with Spark")
		val sqlContext = getSQLContext(sc)
		import sqlContext.implicits._

		val batchDuration = Seconds(4)

		def streamingApp(sc: SparkContext, batchDuration: Duration) = {
			val ssc = new StreamingContext(sc, batchDuration)

			val inputPath = if (isIDE) {
				"file:////Users/omkar/workspace/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
			} else {
				"file:///vagrant/input"
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
			}).cache()

			val activityStateSpec =
				StateSpec
					.function(mapActivityStateFunc)
					.timeout(Minutes(120))

			val stateFulActivityByProduct = activityStream.transform(rdd => {
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
			} ).mapWithState(activityStateSpec)

			//Go through the slides again for the info
			val activityStateSnapshot = stateFulActivityByProduct.stateSnapshots()
			activityStateSnapshot
			    	.reduceByKeyAndWindow(
						(a,b) => b,
						(x,y) => x,
						Seconds(30/4*4)
					)
		    	.foreachRDD(rdd => rdd.map(sr => ActivityByProduct(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3))
				    	.toDF().registerTempTable("ActivityByproduct"))

			//visitor state spec
			val visitorStateSpec =
				StateSpec
		    	.function(mapVisitorsStateFunc)
		    	.timeout(Minutes(120))

			val hll = new HyperLogLogMonoid(12)

			val statefulVisitorsByProduct = activityStream.map(a => {
				((a.product, a.timestamp_hour), hll(a.visitor.getBytes))
			}).mapWithState(visitorStateSpec)

			val visitorStateSnapshot = statefulVisitorsByProduct.stateSnapshots()
			visitorStateSnapshot
			    	.reduceByKeyAndWindow(
						(a,b) => b,
						(x, y) => x,
						Seconds(30/4*4)
					)//only save or expose the snapshot every x seconds. it will help keep the latest value as it progresses
				.foreachRDD(rdd => rdd.map(sr => VisitorsByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate))
		    	.toDF().registerTempTable("VisitorsByProduct")) // foreach will run only every 28 seconds

			ssc
		}

		val ssc = getStreamingContext(streamingApp, sc, batchDuration)
		ssc.start()
		ssc.awaitTermination()

	}

}
