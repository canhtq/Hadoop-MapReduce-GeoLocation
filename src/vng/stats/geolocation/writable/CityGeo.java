package vng.stats.geolocation.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class CityGeo implements WritableComparable {
	private String cityName;
	public String getCityName() {
		return cityName;
	}

	public void setCityName(String cityName) {
		this.cityName = cityName;
	}

	private String countryCode;
	
	public CityGeo() {
		super();
	}

	public CityGeo(String cityName, String countryCode) {
		this.cityName = cityName;
		this.countryCode = countryCode;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.cityName = arg0.readUTF();
		this.countryCode = arg0.readUTF();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(this.cityName);
		arg0.writeUTF(this.countryCode);
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
