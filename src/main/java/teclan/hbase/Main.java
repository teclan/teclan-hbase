package teclan.hbase;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

import teclan.hbase.factory.HbaseFactory;

public class Main {

	private static Configuration configuration;
	private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static void main(String[] args) {

		configuration = HbaseFactory.get(new String[] { "10.0.88.41", "10.0.88.42", "10.0.88.47" }, 2181);

		// createStudentTable();
		// createClassTable();

		addDataForClass();

	}

	private static void addDataForClass() {
		List<Map<String, Object>> keysAndValues = new ArrayList<Map<String, Object>>();

		for (int i = 1; i <= 100; i++) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("id", i);
			map.put("name", "班级" + i);
			map.put("created_at", SDF.format(new Date()));

			keysAndValues.add(map);
		}

		HbaseFactory.addDatas(configuration, "class", "cls", 0, keysAndValues);
	}

	private static void createStudentTable() {

		byte[][] splitKeys = new byte[][] { Bytes.toBytes("10000"), Bytes.toBytes("20000"), Bytes.toBytes("40000"),
				Bytes.toBytes("50000"), Bytes.toBytes("60000"), Bytes.toBytes("70000"), Bytes.toBytes("80000"),
				Bytes.toBytes("90000"), Bytes.toBytes("100000") };

		HbaseFactory.createTable(configuration, "student", new String[] { "stu" }, splitKeys);
	}

	private static void createClassTable() {
		byte[][] splitKeys = new byte[][] { Bytes.toBytes("10"), Bytes.toBytes("20"), Bytes.toBytes("40"),
				Bytes.toBytes("50"), Bytes.toBytes("60"), Bytes.toBytes("70"), Bytes.toBytes("80"), Bytes.toBytes("90"),
				Bytes.toBytes("100") };

		HbaseFactory.createTable(configuration, "class", new String[] { "cls" }, splitKeys);
	}

}
