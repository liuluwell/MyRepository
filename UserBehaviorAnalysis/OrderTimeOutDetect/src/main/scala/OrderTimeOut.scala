import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map
/**
  * @Author lpl
  * @create 2020/7/1 12:35
  */

case class OrderEvent(orderId: Long,eventType: String,eventTime: Long)

case class OrderResult(orderId: Long,eventType: String)
object OrderTimeOut {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orderEventStream: DataStream[OrderEvent] = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(1, "pay", 1558431848),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "pay", 1558430844)
    )).assignAscendingTimestamps(_.eventTime * 1000)


    // 定义一个带匹配时间窗口的模式

    val orderPayPatern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .next("next")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 定义一个输出标签
    val orderTimeOutOutPut: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeout")

    // 订单事件流根据orderId分流，然后在每一条流中匹配出定义好的模式
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream.keyBy(_.orderId),orderPayPatern)

    val complexResult: DataStream[OrderResult] = patternStream.select(orderTimeOutOutPut) {
      // 对于已超时的部分的模式匹配的事件序列，回调用这个函数
      (pattern: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val createOrder: Option[Iterable[OrderEvent]] = pattern.get("begin")
        OrderResult(createOrder.get.iterator.next().orderId, "timeout")
      }
    } {
      // 检测到定义好的模式序列时，就会调用这个函数
      pattern: Map[String, Iterable[OrderEvent]] => {
        val payOrder: Option[Iterable[OrderEvent]] = pattern.get("next")
        OrderResult(payOrder.get.iterator.next().orderId, "success")
      }
    }

    // 拿到同一输出标签中的 timeout 匹配结果

    val timeoutResult: DataStream[OrderResult] = complexResult.getSideOutput(orderTimeOutOutPut)

    complexResult.print()

    timeoutResult.print()

    env.execute("Order Timeout Detect Job")

  }
}
