package teclan.hbase;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import teclan.hbase.factory.HbaseFactory;

public class ClassTableTest {

	private static Configuration configuration;

	@Before
	public void setUp() {
		if (configuration == null) {
			configuration = Support.getConfiguration();
		}
	}

	@After
	public void setDown() {
	}

	@Test
	public void dropTable() {
		long begin = System.currentTimeMillis();
		HbaseFactory.deleteTable(configuration, "cls");
		long end = System.currentTimeMillis();
		System.out.println(Support.getTimeMessage("删除表 cls ", end, begin));
	}

	@Test
	public void createTable() {
		byte[][] splitKeys = new byte[][] { Bytes.toBytes("10"), Bytes.toBytes("20"), Bytes.toBytes("30"),
				Bytes.toBytes("40"), Bytes.toBytes("50"), Bytes.toBytes("60"), Bytes.toBytes("70"), Bytes.toBytes("80"),
				Bytes.toBytes("90"), Bytes.toBytes("100") };

		long begin = System.currentTimeMillis();
		HbaseFactory.createTable(configuration, "cls", new String[] { "cls" }, splitKeys);
		long end = System.currentTimeMillis();
		System.out.println(Support.getTimeMessage("创建表 cls ", end, begin));
	}

	@Test
	public void addDatas() {
		List<Map<String, Object>> keysAndValues = new ArrayList<Map<String, Object>>();

		for (int i = 1; i <= 100; i++) {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("id", i);
			map.put("name", "班级" + i);
			map.put("created_at", Support.SDF.format(new Date()));

			keysAndValues.add(map);
		}
		long begin = System.currentTimeMillis();
		HbaseFactory.addDatas(configuration, "cls", "cls", 0, keysAndValues);
		long end = System.currentTimeMillis();
		System.out.println(Support.getTimeMessage("插入100 条数据 ", end, begin));
	}

}
