package bdma.labos.hadoop.writer;

import java.io.IOException;

public interface MyWriter {
	
	public void open(String file) throws IOException;
	
	public void put(String key, String value);
	
	public void reset();
	
	public int flush() throws IOException;
	
	public void close() throws IOException;
	
}
