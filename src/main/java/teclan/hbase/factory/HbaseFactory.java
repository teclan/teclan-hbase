package teclan.hbase.factory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

public class HbaseFactory {
	private static final Logger LOGEER = LoggerFactory.getLogger(HbaseFactory.class);

	public static Configuration get(String[] quorumIp, int clientPort) {
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", Joiner.on(",").join(quorumIp));
		configuration.set("hbase.zookeeper.property.clientPort", String.valueOf(clientPort));

		return configuration;
	}

	@SuppressWarnings({ "resource", "deprecation" })
	public static void deleteTable(Configuration configuration, String table) {
		try {
			HBaseAdmin admin = new HBaseAdmin(configuration);
			admin.deleteTable(table);
		} catch (IOException e) {
			LOGEER.error(e.getMessage(), e);
		}
	}

	public static void addDatas(Configuration configuration, String table, String family, int startRowKey,
			List<Map<String, Object>> keysAndValues) {

		try {
			Connection connection = ConnectionFactory.createConnection(configuration);
			TableName tableName = TableName.valueOf(table);
			Table tableCreated = connection.getTable(tableName);

			List<Put> puts = new ArrayList<Put>();

			for (int i = 0; i < keysAndValues.size(); i++) {

				Map<String, Object> map = keysAndValues.get(i);
				Put put = new Put(Bytes.toBytes(String.valueOf(startRowKey++)));

				for (String key : map.keySet()) {
					put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(map.get(key).toString()));
				}
				puts.add(put);
			}
			tableCreated.put(puts);
		} catch (IOException e) {
			LOGEER.error(e.getMessage(), e);
		}
	}

	public static void createTable(Configuration configuration, String table, String family) {

		try {
			Connection connection = ConnectionFactory.createConnection(configuration);
			TableName tableName = TableName.valueOf(table);
			connection.getTable(tableName);
		} catch (IOException e) {
			LOGEER.error(e.getMessage(), e);
		}
	}

	@SuppressWarnings({ "resource", "deprecation" })
	public static void createTable(Configuration configuration, String table, String[] familys, byte[][] splitKeys) {
		try {
			HBaseAdmin admin = new HBaseAdmin(configuration);
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(table));
			for (String family : familys) {
				tableDescriptor.addFamily(new HColumnDescriptor(family));
			}
			admin.createTable(tableDescriptor, splitKeys);
		} catch (IOException e) {
			LOGEER.error(e.getMessage(), e);
		}
	}

	@SuppressWarnings({ "resource", "deprecation" })
	public static void createTable(Configuration configuration, String table, String[] familys) {
		try {
			HBaseAdmin admin = new HBaseAdmin(configuration);
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(table));
			for (String family : familys) {
				tableDescriptor.addFamily(new HColumnDescriptor(family));
			}
			admin.createTable(tableDescriptor);
		} catch (IOException e) {
			LOGEER.error(e.getMessage(), e);
		}
	}

}
