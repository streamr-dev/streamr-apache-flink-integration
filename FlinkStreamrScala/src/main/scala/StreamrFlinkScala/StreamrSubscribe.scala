package StreamrFlinkScala

import com.streamr.client.MessageHandler
import com.streamr.client.StreamrClient
import com.streamr.client.Subscription
import com.streamr.client.authentication.ApiKeyAuthenticationMethod
import com.streamr.client.options.StreamrClientOptions
import com.streamr.client.protocol.message_layer.StreamMessage
import com.streamr.client.rest.Stream
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util
import java.util.Map


object StreamrSubscribe {
  private val LOG = LoggerFactory.getLogger(classOf[StreamrSubscribe])
}

class StreamrSubscribe(var apiKey: String, var streamId: String) extends SourceFunction[util.Map[String, AnyRef]] {
  private var client: StreamrClient = null
  private var stream: Stream = null
  private var sub: Subscription = null
  private var waitLock: Object = null
  private var running: Boolean = false

  @throws[Exception]
  override def run(ctx: SourceFunction.SourceContext[util.Map[String, AnyRef]]): Unit = {
    StreamrSubscribe.LOG.info("Initializing Streamr API connection")
    // Connect to Streamr
    client = new StreamrClient(new StreamrClientOptions(new ApiKeyAuthenticationMethod(apiKey)))
    // Get Stream
    try
      stream = client.getStream(streamId)
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
    // Subscribe to the Stream
    streamrSub(ctx)
    StreamrSubscribe.LOG.info("Streamr API connection established successfully")
    // Lock the run method until the application is stopped.
    waitLock = new Object
    running = true
    while ( {
      running
    }) { //            System.out.println(sub.isSubscribed());
      waitLock synchronized waitLock.wait(200L)
      if (client.getState eq StreamrClient.State.Disconnected) client.connect()
      if (!sub.isSubscribed) streamrSub(ctx)

    }
  }

  // Subscribe to the stream
  def streamrSub(ctx: SourceFunction.SourceContext[util.Map[String, AnyRef]]): Unit = {
    this.sub = client.subscribe(stream, new MessageHandler() {
      override def onMessage(subscription: Subscription, streamMessage: StreamMessage): Unit = {
        try // Pass the Stream message to Flink
        ctx.collect(streamMessage.getContent)
        catch {
          case e: IOException =>
            e.printStackTrace()
        }
      }
    })
  }

  override def cancel(): Unit = {
    StreamrSubscribe.LOG.info("Cancelling Streamr source")
    close()
  }

  def close(): Unit = {
    StreamrSubscribe.LOG.info("Closing source")
    client.disconnect()
    this.running = false
    // Leave main method
    waitLock synchronized waitLock.notify()

  }
}
