import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
/**
  * @Author lpl
  * @create 2020/6/30 18:15
  */

case class ApacheLogEvent(op: String,userId: String,eventTime: Long,method: String,url: String)

case class UrlViewCount(url: String,windowEnd: Long,count: Long)


object TrafficAnalysis {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    env.readTextFile("D:\\workspace\\UserBehaviorAnalysis\\TrafficAnalysis\\src\\main\\resources\\apachetest.log")
      .map(line =>
      {
        //83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
        val linearray: Array[String] = line.split(" ")
        val sdf: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = sdf.parse(linearray(3)).getTime
        ApacheLogEvent(linearray(0),linearray(1),timestamp,linearray(5),linearray(6))
      })
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.milliseconds
      (1000)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = {
          element.eventTime
        }
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(1),Time.seconds(5))
      .aggregate( new CountAgg(), new WindowResultFuction())
      .keyBy(_.windowEnd)
      .process( new TopNHotUrl(5) )
      .print()


    env.execute("Traffic Analysis Job")

  }

  class CountAgg extends AggregateFunction[ApacheLogEvent,Long,Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }


  class WindowResultFuction extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{
    override def apply(key: String,
                       window: TimeWindow,
                       input: Iterable[Long],
                       out: Collector[UrlViewCount]): Unit = {
      val url = key
      val count = input.iterator.next
      out.collect(UrlViewCount(url,window.getEnd,count))
    }
  }

  class TopNHotUrl(topSize: Int) extends KeyedProcessFunction[Long,UrlViewCount,String] {

    lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new
        ListStateDescriptor[UrlViewCount]("urlState",classOf[UrlViewCount]))

    override def processElement(value: UrlViewCount,
                                ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
                                out: Collector[String]): Unit = {
      urlState.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 10*1000)
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
                         out: Collector[String]): Unit = {

      val allUrlViews :ListBuffer[UrlViewCount] = ListBuffer()

      import scala.collection.JavaConversions._
      for ( item <- urlState.get()){
        allUrlViews += item
      }

      urlState.clear()

      val sortedUrlViews: ListBuffer[UrlViewCount] = allUrlViews.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

      // 将排名信息格式化成 String, 便于打印
      var result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 10 * 1000)).append("\n")

      for (i <- sortedUrlViews.indices) {
        val currentUrlView: UrlViewCount = sortedUrlViews(i)
        // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
        result.append("No").append(i+1).append(":")
          .append("  URL=").append(currentUrlView.url)
          .append("  流量=").append(currentUrlView.count).append("\n")
      }
      result.append("====================================\n\n")

      Thread.sleep(500)
      out.collect(result.toString())
    }
  }
}

