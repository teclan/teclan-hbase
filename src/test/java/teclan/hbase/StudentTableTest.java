package teclan.hbase;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import teclan.hbase.factory.HbaseFactory;

public class StudentTableTest {

	private static Configuration configuration;

	@Before
	public void setUp() {
		if (configuration == null) {
			configuration = Support.getConfiguration();
		}
	}

	@Test
	public void dropTable() {
		long begin = System.currentTimeMillis();
		HbaseFactory.deleteTable(configuration, "student");
		long end = System.currentTimeMillis();
		System.out.println(Support.getTimeMessage("删除表 student ", end, begin));
	}

	@Test
	public void createTable() {
		byte[][] splitKeys = new byte[][] { Bytes.toBytes("10000"), Bytes.toBytes("20000"), Bytes.toBytes("30000"),
				Bytes.toBytes("40000"), Bytes.toBytes("50000"), Bytes.toBytes("60000"), Bytes.toBytes("70000"),
				Bytes.toBytes("80000"), Bytes.toBytes("90000"), Bytes.toBytes("100000") };

		long begin = System.currentTimeMillis();
		HbaseFactory.createTable(configuration, "student", new String[] { "stu" }, splitKeys);
		long end = System.currentTimeMillis();
		System.out.println(Support.getTimeMessage("创建表 student ", end, begin));

	}

	@Test
	public void addDatas() {
		List<Map<String, Object>> keysAndValues = new ArrayList<Map<String, Object>>();

		for (int i = 1; i <= 100000; i++) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("id", i);
			map.put("name", "学生" + i);
			map.put("cls_id", "班级" + (1 + Math.random() * 100));
			map.put("created_at", Support.SDF.format(new Date()));

			keysAndValues.add(map);
		}

		long begin = System.currentTimeMillis();
		HbaseFactory.addDatas(configuration, "student", "stu", 0, keysAndValues);
		long end = System.currentTimeMillis();
		System.out.println(Support.getTimeMessage("插入100000条数据 ", end, begin));
	}
}
