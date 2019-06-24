package FlinkStreamr;

import com.streamr.client.MessageHandler;
import com.streamr.client.StreamrClient;
import com.streamr.client.Subscription;
import com.streamr.client.authentication.ApiKeyAuthenticationMethod;
import com.streamr.client.options.StreamrClientOptions;
import com.streamr.client.protocol.message_layer.StreamMessage;
import com.streamr.client.rest.Stream;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class StreamrSubscribe implements SourceFunction<Map<String, Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(StreamrSubscribe.class);
    private StreamrClient client;
    private Stream stream;
    private Subscription sub;
    private String apiKey;
    private String streamId;
    private Object waitLock;
    private volatile boolean running;

    public StreamrSubscribe(String apiKey, String streamId) {
        this.apiKey = apiKey;
        this.streamId = streamId;

    }

//    @Override
//    public void open(Configuration parametres) throws Exception{
//        waitLock = new Object();
//    }

    @Override
    public void run(final SourceContext<Map<String, Object>> ctx) throws Exception {
        LOG.info("Initializing Streamr API connection");
        client = new StreamrClient(new StreamrClientOptions(new ApiKeyAuthenticationMethod(apiKey)));
        try {
            stream = client.getStream(streamId);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.sub = client.subscribe(stream, new MessageHandler() {
            @Override
            public void onMessage(Subscription subscription, StreamMessage streamMessage) {
//                System.out.println(streamMessage);
                try {
                    ctx.collect(streamMessage.getContent());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        LOG.info("Streamr API connection established successfully");

        waitLock = new Object();
        running = true;
        while (running) {
            synchronized (waitLock) {
                waitLock.wait(100L);
            }
        }
    }

    @Override
    public void cancel() {
        LOG.info("Cancelling Streamr source");
        close();
    }

    public void close() {
        LOG.info("Closing source");
        client.disconnect();
        this.running = false;

        // Leave main method
        synchronized (waitLock) {
            waitLock.notify();
        }
    }


}
