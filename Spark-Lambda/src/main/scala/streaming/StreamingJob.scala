package streaming

import _root_.kafka.serializer.StringDecoder
import com.twitter.algebird.HyperLogLogMonoid
import config.Settings
import domain.{ActivityByProduct, VisitorsByProduct}
import functions._
import kafka.common.TopicAndPartition
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.max
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
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
			val wlc = Settings.WebLogGen
			val topic = wlc.kafkaTopic

			/**
			  * Receiver based approach
			  * val kafkaParams = Map(
				"zookeeper.connect" -> "localhost:2181",
				"group.id" -> "lambda",
				"auto.offset.reset" -> "largest")
			  */


			/**
			  *val kafkaStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
				ssc, kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_AND_DISK)
		    	.map(_._2)
			  **/

			//driver based approach
			val kafkaDirectParams = Map(
				"metadata.broker.list" -> "localhost:9092",
				"group.id" -> "lambda",
				"auto.offset.reset" -> "smallest" //check this again
			)

			//to store offsets in hdfs
			val hdfsPath = wlc.hdfsPath
			val hdfsData = sqlContext.read.parquet(hdfsPath)

			//.collect() gives back row object
			val fromOffsets = hdfsData.groupBy("topic", "kafkaPartition").agg(max("untilOffset").as("untilOffset"))
				.collect().map{ row =>
				(TopicAndPartition(row.getAs[String]("topic"), row.getAs[Int]("kafkaPartition")), row.getAs[String]("untilOffset").toLong + 1)
			}.toMap
			/**
			  * the conversion to offset ranges has to be first thing to happen
			  * Hence, it needs to be done before applying map
			  */

			/**
			  * we are going to send a (k,v) pair of topic, data
			  * and accordingly map it to spark partition based on offsetRange
			  * TODO: go through this again
			  */
			val kafkaDirectStream = fromOffsets.isEmpty match {
				case true =>
					KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
						ssc, kafkaDirectParams, Set(topic))
					case false =>
						KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String) ](
							ssc, kafkaDirectParams, fromOffsets, {mmd => (mmd.key(), mmd.message()) }
						)
			}


			val activityStream = kafkaDirectStream.transform(input => {
				functions.rddToRDDActivity(input)
			}).cache()

			//we are going to fork the data pipeline here, one for batch and one for speed layer
			/**
			  * we are going to convert activity stream to DF store it on HDFS
			  */

			/**
			  * selectExpr can help add some code for every column
			  */

			//this is batch process. This needs to be in different application.
			//Use kafka connect to connect it to hadoop
			activityStream.foreachRDD{ rdd => {
				val activityDF = rdd
					.toDF()
			    	.selectExpr("timestamp_hour", "referrer", "action", "prevPage", "page", "visitor", "product",
						"inputProps.topic as topic", "inputProps.kafkaPartition as kafkaPartition",
						"inputProps.fromOffset as fromOffset", "inputProps.untilOffset as untilOffset")
				activityDF
					.write
					.partitionBy("topic", "kafkaPartition", "timestamp_hour")
			    	.mode(SaveMode.Append)
			    	.parquet("hdfs://lambda-pluralsight:9000/lambda/weblogs-app1/")
			}}

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
