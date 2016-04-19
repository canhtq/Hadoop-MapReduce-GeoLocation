package vng.stats.geolocation.geoip;

import com.maxmind.geoip2.DatabaseReader;

public class GeoIP {
	private static GeoIP instance = null;
	private DatabaseReader reader;
	public GeoIP() {
		
	}

	public static GeoIP getInstance() {
		if (instance == null) {
			instance = new GeoIP();
		}
		return instance;
	}
	
}
