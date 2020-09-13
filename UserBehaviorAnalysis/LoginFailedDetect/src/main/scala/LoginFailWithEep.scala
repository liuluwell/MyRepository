import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.windowing.time.Time
import scala.collection.Map
/**
  * @Author lpl
  * @create 2020/7/1 11:57
  */
object LoginFailWithEep {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginEventStream: DataStream[LoginEvent] = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430832),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.eventTime * 1000)

    // 定义匹配模式
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .within(Time.seconds(2))

    // 在数据流中匹配出定义好的模式

    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId),loginFailPattern)

    // .select方法传入一个pattern select function， 当检测到定义好的模式序列时就会调用
    val loginFailDataStrream: DataStream[(Long, String, String)] = patternStream.select((pattern: Map[String, Iterable[LoginEvent]]) => {
      val first: LoginEvent = pattern.getOrElse("begin", null).iterator.next()
      val second: LoginEvent = pattern.getOrElse("next", null).iterator.next()
      (second.userId, second.ip, second.eventType)
    })

    loginFailDataStrream.print()

    env.execute("Login Fail Detect Job")

  }



}
