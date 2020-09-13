import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


/**
  * @Author lpl
  * @create 2020/6/30 14:40
  */

case class UserBehavior(userId: Long,itemId: Long,catagoryId: Int,behavior: String,timestamp: Long)

case class ItemViewCount(itemId: Long,windowEnd: Long,count: Long)

object HotItems {

  def main(args: Array[String]): Unit = {

    // 创建一个StreamExecutionEnvironment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设定Time类型为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置并发度为1
    env.setParallelism(1)

    env
      .readTextFile("D:\\workspace\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(
        line => {
          val linearray: Array[String] = line.split(",")
          UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
        })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior=="pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1),Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy(1)
      .process(new TopNHotItems(3))
      .print




    env.execute("Hot Item Job")

  }

  class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class WindowResultFunction extends WindowFunction[Long,ItemViewCount,Long,TimeWindow] {
    override def apply(key: Long,
                       window: TimeWindow,
                       aggregateResult: Iterable[Long],
                       collector: Collector[ItemViewCount]): Unit = {
      val itemId: Long = key
      val count: Long = aggregateResult.iterator.next
      collector.collect(ItemViewCount(itemId,window.getEnd,count))
    }
  }


   class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String]{

     private var itemState : ListState[ItemViewCount] = _


     override def open(parameters: Configuration): Unit = {
       super.open(parameters)
       // 命名状态变量的名字和状态变量的类型
       val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState",classOf[ItemViewCount])
       // 从运行上下文中获取状态并赋值
       itemState = getRuntimeContext.getListState(itemStateDesc)
     }

     override def processElement(value: ItemViewCount,
                                 ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context,
                                 out: Collector[String]): Unit = {
       // 每条数据保存到状态中
       itemState.add(value)

       // 注册windowEnd + 1 的EventTime Timer，当触发时，说明收集了所有windowEnd窗口的所有商品数据
       // 也就是当程序看到windowEnd+1的水位线watermark时，触发onTimer回调函数
       ctx.timerService().registerEventTimeTimer(value.windowEnd+1)


     }

     override def onTimer(timestamp: Long,
                          ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext,
                          out: Collector[String]): Unit = {

       // 获取收到的所有商品的点击量
       val allItems: ListBuffer[ItemViewCount] = ListBuffer()
       import scala.collection.JavaConversions._
       for( item <- itemState.get){
         allItems += item
       }

       // 清楚状态中的数据，释放空间
       itemState.clear()

       // 排序
       val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

       // 将排名信息格式化成 String, 便于打印
       val result: StringBuilder = new StringBuilder
       result.append("====================================\n")
       result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

       for(i <- sortedItems.indices){
         val currentItem: ItemViewCount = sortedItems(i)
         // e.g.  No1：  商品ID=12224  浏览量=2413
         result.append("No").append(i+1).append(":")
           .append("  商品ID=").append(currentItem.itemId)
           .append("  浏览量=").append(currentItem.count).append("\n")
       }
       result.append("====================================\n\n")
       // 控制输出频率，模拟实时滚动结果
       Thread.sleep(1000)
       out.collect(result.toString)
     }
   }

}
