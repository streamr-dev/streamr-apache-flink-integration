package FlinkStreamr;

import com.streamr.client.StreamrClient;
import com.streamr.client.authentication.ApiKeyAuthenticationMethod;
import com.streamr.client.options.StreamrClientOptions;
import com.streamr.client.rest.Stream;
import com.streamr.client.rest.StreamConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.IOException;
import java.util.Map;

public class StreamrPublish extends RichSinkFunction<Map<String, Object>> {
    private String apiKey;
    private String streamId;
    private StreamrClient client;
    private Stream stream;

    public StreamrPublish(String apiKey, String stringId) {
        this.apiKey = apiKey;
        this.streamId = stringId;
    }
    @Override
    public void open(Configuration parametres) throws Exception{
        client = new StreamrClient(new StreamrClientOptions(new ApiKeyAuthenticationMethod(apiKey)));
        try {
            stream = client.getStream(streamId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void invoke(Map<String, Object> in, Context ctx) {
        if (client.getState() == StreamrClient.State.Disconnected) {
            client.connect();
        }
        client.publish(stream, in);
    }

}
