package vng.stats.geolocation.jobs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class IpLocationReduce extends Reducer<Text, IntWritable, Text, Text> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

	};

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int total =0;
		for (IntWritable value : values) {
			total+=value.get();
		}
		String strKey = key.toString();
		String[] keyParts = strKey.split("(#)");
		String data = String.format("%s	%s	%s	%d",keyParts[1],keyParts[2],keyParts[3], total);
		context.write(new Text(keyParts[0]), new Text(data));
	}
}
