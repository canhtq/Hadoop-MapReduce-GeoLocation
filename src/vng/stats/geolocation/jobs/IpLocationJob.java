package vng.stats.geolocation.jobs;

import java.util.Calendar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class IpLocationJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		//conf.addResource(new Path(System.getProperty("oozie.action.conf.xml")));
		conf.set("geo.mmdb", "/user/zdeploy/ub/geoip2/GeoLite2-City.mmdb");
		//conf.set("mapreduce.job.queuename","production");
		conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
		conf.set("index.ip.inlog",args[2]);
		conf.set("game.code",args[3]);
		conf.set("log.date",args[4]);
		Job job = Job.getInstance(conf,"GeoIpLocationJob");
		job.setJarByClass(IpLocationJob.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(IpLocationMap.class);
	    job.setReducerClass(IpLocationReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setNumReduceTasks(4);
	    boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new IpLocationJob(), args);
        System.exit(res);
    }
}
