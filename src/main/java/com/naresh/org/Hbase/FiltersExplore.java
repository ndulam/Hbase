package com.naresh.org.Hbase;
import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
public class FiltersExplore {

	public static void main(String[] args) 
	{
		Configuration config = HBaseConfiguration.create();
		config.addResource(new Path("/Users/nd2629/Downloads/hbase-1.4.1/conf/hbase-site.xml"));
	
		try {
			Connection connection = ConnectionFactory.createConnection(config);
			Admin admin = connection.getAdmin();
			TableName tablename = TableName.valueOf("test_messages");
			Table table = connection.getTable(tablename);
			byte[] row1 = Bytes.toBytes("row2");
			Get g = new Get(row1);
			//Result r = table.get(g);
			//byte[] value = r.getValue("main".getBytes(), "cs".getBytes());
			//System.out.println(Bytes.toString(value));
			
			FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			
			SingleColumnValueFilter filter1 = new SingleColumnValueFilter("main".getBytes(),"pa".getBytes(),CompareFilter.CompareOp.LESS_OR_EQUAL,Bytes.toBytes("10"));
			filter1.setFilterIfMissing(true);
			
			SingleColumnValueFilter filter2 = new SingleColumnValueFilter("main".getBytes(),"cs".getBytes(),CompareFilter.CompareOp.EQUAL,Bytes.toBytes("10"));
			filter2.setFilterIfMissing(true);
			
			SingleColumnValueFilter filter3 = new SingleColumnValueFilter("main".getBytes(),"eps".getBytes(),CompareFilter.CompareOp.EQUAL,Bytes.toBytes("10"));
			filter3.setFilterIfMissing(true);
			
			filterList.addFilter(filter1);
			filterList.addFilter(filter2);
			filterList.addFilter(filter3);
			
			//MultipleColumnPrefixFilter mcpf = new MultipleColumnPrefixFilter(new byte[][] {Bytes.toBytes("pa"),Bytes.toBytes("cs"),Bytes.toBytes("eps")});
			MultipleColumnPrefixFilter mcpf = new MultipleColumnPrefixFilter(new byte[][] {Bytes.toBytes("pa"),Bytes.toBytes("eps")});
			filterList.addFilter(mcpf);
			
		  Scan scan = new Scan();
		  scan.setFilter(filterList);
	
			  ResultScanner scanner = table.getScanner(scan);
	            int i = 0;
	            for (Result result : scanner) {
	            	System.out.println("Inside loop");
	            	NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes("main"));
	            String[] Quantifers = new String[familyMap.size()];
	            String columnName;
	            for(byte[] bQunitifer : familyMap.keySet())
	            	{
	            	columnName  = Bytes.toString(bQunitifer);
	            	 System.out.println("qualifier "+columnName+ " "+ Bytes.toString(result.getValue("main".getBytes(),columnName.getBytes())));

	            	 }
	            
	           	System.out.println("About to exit loop");

	            }

	}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
	

