package com.naresh.org.Hbase;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
public class HbaseTestConnectivity {

	public static void main(String[] args) 
	{
		Configuration config = HBaseConfiguration.create();
		config.addResource(new Path("/Users/nd2629/Downloads/hbase-1.4.1/conf/hbase-site.xml"));
		//config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
	
		try {
			//HTable pageViewTable = new HTable( config, "PageViews");
			Connection connection = ConnectionFactory.createConnection(config);
			Admin admin = connection.getAdmin();
			TableName table1 = TableName.valueOf("test_messages");
			Table table = connection.getTable(table1);
			byte[] row1 = Bytes.toBytes("row2");
			Get g = new Get(row1);
			Result r = table.get(g);
			byte[] value = r.getValue("main".getBytes(), "cs".getBytes());
			System.out.println(Bytes.toString(value));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
