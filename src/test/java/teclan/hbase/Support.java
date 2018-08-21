package teclan.hbase;

import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;

import teclan.hbase.factory.HbaseFactory;

public class Support {

	public static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static Configuration getConfiguration() {
		return HbaseFactory.get(new String[] { "10.0.88.41", "10.0.88.42", "10.0.88.47" }, 2181);
	}

	public static String getTimeMessage(String message, long end, long begin) {
		return String.format("%s 耗时 %s 秒", message, (end - begin) * 1.0 / 1000);
	}

}
