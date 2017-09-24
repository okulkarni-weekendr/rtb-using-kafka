/**
  * Created by Ahmad Alkilani on 5/1/2016.
  */
package object domain {
    case class Activity(timestamp_hour: Long,
                        referrer: String,
                        action: String,
                        prevPage: String,
                        visitor: String,
                        page: String,
                        product: String,
                        inputProps: Map[String, String] = Map()
                       )

    case class ActivityByProduct(product: String,
                                 timestamp_hour: Long,
                                 purchase_count: Long,
                                 add_to_cart_count: Long,
                                 page_view_count: Long
                                )

    case class VisitorsByProduct(product: String, timestamp_hour: Long, unique_visitors: Long)
}
