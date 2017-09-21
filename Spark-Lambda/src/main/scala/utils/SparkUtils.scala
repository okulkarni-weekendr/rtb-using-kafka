package utils

import java.lang.management.ManagementFactory

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtils {
	val isIDE = {
		ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")
	}

	def getSparkContext(appName: String) = {
		var checkpointDirectory = ""

		// get spark configuration
		val conf = new SparkConf()
			.setAppName(appName)

		// Check if running from IDE
		    if (isIDE) {
		      conf.setMaster("local[*]")
		      checkpointDirectory = "file:////Users/omkar/workspace/spark-kafka-cassandra-applying-lambda-architecture/vagrant/input"
		    } else {
		      checkpointDirectory = "hdfs://lambda-pluralsight:9000/spark/checkpoint"
		    }

		// setup spark context
		val sc = SparkContext.getOrCreate(conf)
		sc.setCheckpointDir(checkpointDirectory)
		sc
	}

	def getSQLContext(sc: SparkContext) = {
		val sqlContext = SQLContext.getOrCreate(sc)
		sqlContext
	}

	def getStreamingContext(streamingApp : (SparkContext, Duration) => StreamingContext, sc : SparkContext, batchDuration: Duration) = {
		val creatingFunc = () => streamingApp(sc, batchDuration)
		val ssc = sc.getCheckpointDir match {
			case Some(checkpointDir) => StreamingContext.getActiveOrCreate(checkpointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
			case None => StreamingContext.getActiveOrCreate(creatingFunc)
		}
		sc.getCheckpointDir.foreach( cp => ssc.checkpoint(cp))
		ssc
	}
}
