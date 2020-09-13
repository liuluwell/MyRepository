import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
/**
  * @Author lpl
  * @create 2020/7/1 11:37
  */

case class LoginEvent(userId: Long,ip: String,eventType: String,eventTime: Long)

object LoginFail {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.eventTime*1000)
      .keyBy(_.userId)
      .process(new MethodFunction())
      .print

    env.execute("Login Fail Detect Job")
  }

  class MethodFunction() extends KeyedProcessFunction[Long,LoginEvent,LoginEvent] {

    // 定义状态变量
    lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]
    ("loginState",classOf[LoginEvent]))

    override def processElement(lgoin: LoginEvent,
                                ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context,
                                out: Collector[LoginEvent]): Unit = {

      if(lgoin.eventType == "fail"){
        loginState.add(lgoin)
      }

      // 注册定时器 触发事件设定为2秒后
      ctx.timerService().registerEventTimeTimer(lgoin.eventTime+2*1000)

    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext,
                         out: Collector[LoginEvent]): Unit = {
      // 定义一个 LIstBuffer
      val allLogins: ListBuffer[LoginEvent] = ListBuffer()
      import scala.collection.JavaConversions._

      for (login <- loginState.get()){
        allLogins += login
      }

      loginState.clear()

      if(allLogins.length>=2){
        out.collect(allLogins.head)
      }

    }
  }

}
