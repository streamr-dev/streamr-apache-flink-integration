package StreamrFlinkScala

import com.streamr.client.StreamrClient
import com.streamr.client.authentication.ApiKeyAuthenticationMethod
import com.streamr.client.options.StreamrClientOptions
import com.streamr.client.rest.Stream
import com.streamr.client.rest.StreamConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import java.io.IOException
import java.util
import java.util.Map


class StreamrPublish(var apiKey: String, var streamId: String) extends RichSinkFunction[util.Map[String, AnyRef]] {
  var client: StreamrClient = null
  var stream: Stream = null

  @throws[Exception]
  override def open(parametres: Configuration): Unit = {
    client = new StreamrClient(new StreamrClientOptions(new ApiKeyAuthenticationMethod(apiKey)))
    try
      stream = client.getStream(streamId)
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

  override def invoke(in: util.Map[String, AnyRef], ctx: SinkFunction.Context[_]): Unit = {
    if (client.getState eq StreamrClient.State.Disconnected) client.connect()
    client.publish(stream, in)
  }
}
