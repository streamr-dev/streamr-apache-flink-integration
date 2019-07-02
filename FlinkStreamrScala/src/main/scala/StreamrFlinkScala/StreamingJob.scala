package StreamrFlinkScala

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import java.util
import java.util.Map


object StreamingJob {
  @throws[Exception]
  def main(args: Array[String]): Unit = { // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val streamrSub = new StreamrSubscribe("YOUR_STREAMR_API_KEY", "SUB_STREAM_ID")
    val streamPub = new StreamrPublish("YOUR_STREAMR_API_KEY", "PUB_STREAM_ID")
    val tramDataSource = env.addSource(streamrSub)
    // Filter the 6T trams.
    val stream = tramDataSource.filter(new FilterFunction[util.Map[String, AnyRef]]() {
      @throws[Exception]
      override def filter(s: util.Map[String, AnyRef]): Boolean = s.containsKey("desi") && s.get("desi").toString.contains("6")
    })
    // Add the publish sink
    stream.print
    stream.addSink(streamPub)
    env.execute("Trams")
  }
}
