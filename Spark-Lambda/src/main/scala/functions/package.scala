import com.twitter.algebird.{HLL, HyperLogLogMonoid}
import domain.{Activity, ActivityByProduct}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.State
import org.apache.spark.streaming.kafka.HasOffsetRanges

package object functions {
	def mapActivityStateFunc = (k: (String, Long), v: Option[ActivityByProduct], state: State[(Long, Long, Long)]) => {
		var (purchase_count, add_to_cart_count, page_view_count) = state.getOption().getOrElse((0L, 0L, 0L))

		val newVal = v match {
			case Some( a: ActivityByProduct ) => (a.purchase_count, a.add_to_cart_count, a.page_view_count)
			case _ => (0L, 0L, 0L)
		}

		purchase_count += newVal._1
		add_to_cart_count += newVal._2
		page_view_count += newVal._3

		state.update((purchase_count, add_to_cart_count, page_view_count))

		val underExposed = {
			if(purchase_count == 0)
				0
			else
				page_view_count / purchase_count
		}
		underExposed
	}

	def mapVisitorsStateFunc = (k: (String, Long), v: Option[HLL], state: State[HLL]) => {
		val currentVisitorHLL = state.getOption().getOrElse(new HyperLogLogMonoid(12).zero)
		val newVisitorHLL = v match {
			case Some(visitorHLL) => currentVisitorHLL + visitorHLL
				case None => currentVisitorHLL
		}
		state.update(newVisitorHLL)
		val output = newVisitorHLL.approximateSize.estimate
		output
	}

	def rddToRDDActivity(input: RDD[(String, String)]) = {

		/**
		  * To do 1:1 mapping of kafka to spark partition:
		  * instance of RDD is actually instance of HasOffsetRanges class.
		  * offsetRanges are indexed by partition no. of spark
		  */
		val offsetRanges = input.asInstanceOf[HasOffsetRanges].offsetRanges

		input.mapPartitionsWithIndex({ (index, it) =>
			val or = offsetRanges(index)
			it.flatMap { kv =>
				val line = kv._2
				val record = line.split("\\t")
				val MS_IN_HOUR = 1000 * 60 * 60
				if (record.length == 7)
					Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6),
						Map("topic" -> or.topic.toString, "kafkaPartition" -> or.partition.toString, "untilOffset" -> or.untilOffset.toString)))
				else
					None
			}
		})
	}
}
