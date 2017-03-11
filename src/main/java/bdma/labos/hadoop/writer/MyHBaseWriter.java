package bdma.labos.hadoop.writer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;

public class MyHBaseWriter implements MyWriter {

	private Configuration config;
	private Connection connection;

	protected BufferedMutator buffer;
	protected int key;
	protected HashMap<String, String> data;
		
	public MyHBaseWriter() {
		this.key = 0;
		this.reset();
	}

	public void open(String tableName) throws IOException {
		this.config = HBaseConfiguration.create();
		this.connection = ConnectionFactory.createConnection(this.config);
		this.buffer = this.connection.getBufferedMutator(TableName.valueOf(tableName));
	}
	
	public void put(String attribute, String value) {
		this.data.put(attribute, value);
	}
	
	public void reset() {
		this.data = new HashMap<String, String>();
	}
	
	protected String nextKey() {
		return String.valueOf(this.key);
	}
	
	protected String toFamily(String attribute) {
		return "all";
	}
	
	public int flush() throws IOException {
		// Obtain the row key
		String rowKey = this.nextKey();
		System.out.println("Row with key "+rowKey+" outputted");
		
		// Create a new Put object with an incremental key
		Put put = new Put(rowKey.getBytes());
		
		// Now get all the columns
		Set< Entry<String, String> > entries = this.data.entrySet();
		int length = 0;
		for (Entry<String, String> entry : entries) {
			// Add the value in the Put object
			String attribute = entry.getKey();
			String family = this.toFamily(attribute);
			String value = entry.getValue();
			put.addColumn(family.getBytes(), attribute.getBytes(), value.getBytes());
		
			length += value.length();
		}
		// Insert it!
		this.buffer.mutate(put);
		
		this.key++;
		this.reset();
		return length;
	}
	
	public void close() throws IOException {
		this.buffer.flush();
		this.buffer.close();
		this.connection.close();
	}
	
}
