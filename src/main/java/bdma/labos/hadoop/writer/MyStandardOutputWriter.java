package bdma.labos.hadoop.writer;

import java.io.IOException;

public class MyStandardOutputWriter implements MyWriter {
	
	private StringBuilder buffer;
	private boolean first;
	
	public MyStandardOutputWriter() {
		this.reset();
	}
	
	public void open(String file) throws IOException {}
	
	public void put(String key, String value) {
		if (first) {
			this.buffer.append(value);
			first = false;
		}
		else {
			this.buffer.append(","+value);
		}
	}
	
	public void reset() {
		this.buffer = new StringBuilder();
		this.first = true;
	}
	
	public int flush() throws IOException {
		String data = this.buffer.toString();
		System.out.println(data);
		this.reset();
		return data.length();
	}
	
	public void close() throws IOException {}
	
}
