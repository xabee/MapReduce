package bdma.labos.hadoop;

import java.util.Random;

public class Generator {
	
	private Random rand;
	
	public Generator(int seed) {
		this.rand = new Random(seed);
	}
	
	public String nextValue(int min, int max) {
		double value = (rand.nextInt((max-min)*1000000)+min*1000000)/(double)1000000;
		return String.valueOf(value);
	}
	
	public String nextType() {
		int type = rand.nextInt(3)+1;
		if (type == 1) {
			return "type_1";
		}
		else if (type == 2) {
			return "type_2";
		}
		else if (type == 3) {
			return "type_3";
		}
		return "type_NA";
	}
	
	public String nextRegion(String type) {
		if (type.equals("type_1")) {
			return String.valueOf(rand.nextInt(10));
		}
		else if (type.equals("type_2")) {
			return String.valueOf(rand.nextInt(10*10));
		}
		else if (type.equals("type_3")) {
			return String.valueOf(rand.nextInt(10*10*10));
		}
		return "region_NA";
	}
	
}
