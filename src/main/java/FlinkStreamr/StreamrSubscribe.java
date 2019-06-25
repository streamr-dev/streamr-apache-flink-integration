package FlinkStreamr;

import com.streamr.client.MessageHandler;
import com.streamr.client.StreamrClient;
import com.streamr.client.Subscription;
import com.streamr.client.authentication.ApiKeyAuthenticationMethod;
import com.streamr.client.options.StreamrClientOptions;
import com.streamr.client.protocol.message_layer.StreamMessage;
import com.streamr.client.rest.Stream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Map;

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

    @Override
    public void run(final SourceContext<Map<String, Object>> ctx) throws Exception {
        LOG.info("Initializing Streamr API connection");

        // Connect to Streamr
        client = new StreamrClient(new StreamrClientOptions(new ApiKeyAuthenticationMethod(apiKey)));
        // Get Stream
        try {
            stream = client.getStream(streamId);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Subscribe to the Stream
        streamrSub(ctx);

        LOG.info("Streamr API connection established successfully");

        // Lock the run method until the application is stopped.
        waitLock = new Object();
        running = true;
        while (running) {
//            System.out.println(sub.isSubscribed());
            synchronized (waitLock) {
                waitLock.wait(200L);
                if (!sub.isSubscribed()) {
                    streamrSub(ctx);
                }
            }
        }
    }

    // Subscribe to the stream
    public void streamrSub(final SourceContext<Map<String, Object>> ctx) {
        this.sub = client.subscribe(stream, new MessageHandler() {
            @Override
            public void onMessage(Subscription subscription, StreamMessage streamMessage) {
                try {
                    // Pass the Stream message to Flink
                    ctx.collect(streamMessage.getContent());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
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
