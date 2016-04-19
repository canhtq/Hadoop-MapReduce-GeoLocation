package vng.stats.geolocation.jobs;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Country;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class IpLocationMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final Logger logger_ = Logger.getLogger(IpLocationMap.class);
	private DatabaseReader reader=null;
	
	private int ipIndex=0;
	private String gameCode="";
	private String logDate="";
	private String parseIp(String rawIp){
		String ip = rawIp;
		String[] s=rawIp.split("(:)");
		if(s.length>0){
			//case 183.182.126.36:20962
			ip = s[0];
		}
		if(ip!=null){
			ip=ip.trim();
		}
		return ip;
	}
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		String fileGeoDb = context.getConfiguration().get("geo.mmdb");
		String geoIndex= context.getConfiguration().get("index.ip.inlog");
		gameCode= context.getConfiguration().get("game.code");
		logDate= context.getConfiguration().get("log.date");
		
		ipIndex = Integer.parseInt(geoIndex);
		Path geodbPath = new Path(fileGeoDb);
		FileSystem fs = FileSystem.get(context.getConfiguration());
		if (fs.exists(geodbPath)) {
			int fileLen = (int)fs.getFileStatus(geodbPath).getLen();
			InputStream is = fs.open(geodbPath);
			byte [] buf = new byte[fileLen];
			IOUtils.readFully(is,buf,0, buf.length);
			InputStream myInputStream = new ByteArrayInputStream(buf); 
			IOUtils.closeStream(is);
			reader = new DatabaseReader.Builder(myInputStream).build();
		}
		
	};
	public static String createKey(String... parts) {
		String key = "";
		int n = parts.length;
		int i = 0;
		for (String part : parts) {
			if (i < n - 1) {
				key += part + "#";
			} else {
				key += part;
			}
			i++;
		}
		return key;
	}
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] items = line.split("\t");
		if(items.length>ipIndex){
			String rawIp = items[ipIndex];
			String ip = parseIp(rawIp);
			InetAddress ipAddress=null;
			try{
				ipAddress = InetAddress.getByName(ip);
			}catch (UnknownHostException uex){
				context.write(new Text(createKey("IPW","IPW","IPW","IPW")), new IntWritable(1));
			}
			try {
				
				CityResponse response = reader.city(ipAddress);
				if(response!=null){
					Country country = response.getCountry();
					City city = response.getCity();
					if(city.getName()!=null){
						context.write(new Text(createKey(gameCode,logDate,country.getIsoCode(),city.getName())), new IntWritable(1));
					}
				}else{
					context.write(new Text(createKey("NA","NA","NA","NA")), new IntWritable(1));
				}
			} catch (GeoIp2Exception e) {
				context.write(new Text(createKey("GeoW","GeoW","GeoW","GeoW")), new IntWritable(1));
			}
			
		}
	}
}
