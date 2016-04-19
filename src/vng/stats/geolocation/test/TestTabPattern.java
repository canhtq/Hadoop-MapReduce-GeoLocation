package vng.stats.geolocation.test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestTabPattern {
	
	private static boolean checkIpV4Format(String ip){
		String[] parts = ip.split("(.)");
		if(parts.length==4){
			return true;
		}
		return false;
	}
	public static void main(String[] args) throws UnknownHostException {
		String regex = "(?:[^\\t]*)\\t(?:[^\\t]*)\\t([^\\t]*)\\t(?:[^\\t]*)";
		Pattern pattern = Pattern.compile(regex);

		Matcher matcher = pattern.matcher("asaas	asdsad	asdsad");
		if (matcher.matches()) {
		  System.err.println(matcher.group(1));
		}
		String ip="123.20.22.58 : 16197";
		String[] s=ip.split("(:)");
		System.err.println(s[0].trim());
	}
}
