
package FlinkStreamr;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamrSubscribe streamrSub = new StreamrSubscribe("YOUR_STREAMR_API_KEY", "SUBSCRIBE_STREAM_ID");
		StreamrPublish streamPub = new StreamrPublish("YOUR_STREAMR_API_KEY", "PUBLISH_STREAM_ID");

		DataStreamSource<Map<String, Object>> tramDataSource = env.addSource(streamrSub);
		// Filter the 6T trams.
		DataStream<Map<String, Object>> stream = tramDataSource.filter(new FilterFunction<Map<String, Object>>() {
			@Override
			public boolean filter(Map<String, Object> s) throws Exception {
				return (s.containsKey("desi") && s.get("desi").toString().contains("6"));
			}
		});
		// Add the publish sink
		stream.addSink(streamPub);

		env.execute("Trams");

	}
}
