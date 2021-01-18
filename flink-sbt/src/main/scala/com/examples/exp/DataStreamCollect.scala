package com.examples.exp

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.cep.{CEP, PatternSelectFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.JavaConverters._


object DataStreamCollect {

  case class Data
  (
    time: Long,
    key: Long,
    value: Long,
    delta: Long,
    list: java.util.List[String],
  ) {
    def withList(newList: Seq[String]) = Data(
      time = time,
      key = key,
      value = value,
      delta = delta,
      list = newList.asJava,
    )
  }

  def main(args: Array[String]): Unit = {
    println("DataStreamCollect")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream1 = env
      .fromCollection(1 to 20)
      .map(x => Data(x * 1000, x % 2, x, -999, Seq(s"$x").asJava))
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Data](Time.seconds(60)) {
        override def extractTimestamp(element: Data): Long = {
          element.time
        }
      })

    val stream = sortedOf(stream1)
      .keyBy(_.key)
      .countWindow(2, 1)
      .reduce((a, b) => {
        val data = Data(b.time, b.key, b.value, b.value - a.value, Seq(s"(${a.value}-${b.value})").asJava)
        data
        //        if (a.value != b.value) data
        //        else data.withList(Seq())
      })
      .keyBy(_.key)
      .timeWindow(Time.seconds(10))
      .reduce((a, b) => (if (a.time < b.time) b else a).withList(a.list.asScala ++ b.list.asScala))
      .keyBy(_.key)
      .map(x => s"v$x")

    val result = DataStreamUtils.collect(stream.javaStream).asScala.toList

    result.foreach(x => {
      println(s"value: $x")
    })
  }

  def sortedOf[T: TypeInformation](stream: DataStream[T]): DataStream[T] = {
    val pattern = Pattern.begin[T]("any").where(new SimpleCondition[T] {
      override def filter(t: T): Boolean = true
    })
    val sorted = CEP.pattern(stream.javaStream, pattern).select(new PatternSelectFunction[T, T]() {
      override def select(map: java.util.Map[String, java.util.List[T]]): T = {
        map.get("any").get(0)
      }
    })

    new DataStream[T](sorted)
  }
}
