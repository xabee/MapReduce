package bdma.labos.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;

import bdma.labos.hadoop.mapreduce.CartesianMapReduce;
import bdma.labos.hadoop.mapreduce.GroupByAndAggMapReduce;
import bdma.labos.hadoop.mapreduce.JobMapReduce;
import bdma.labos.hadoop.mapreduce.JoinMapReduce;
import bdma.labos.hadoop.mapreduce.OneNNMapReduce_1;
import bdma.labos.hadoop.mapreduce.OneNNMapReduce_2;
import bdma.labos.hadoop.mapreduce.ProjectionMapReduce;
import bdma.labos.hadoop.mapreduce.SelectionMapReduce;
import bdma.labos.hadoop.reader.MyHBaseReader;
import bdma.labos.hadoop.reader.MyHBaseReader_C_1;
import bdma.labos.hadoop.reader.MyHBaseReader_C_2;
import bdma.labos.hadoop.reader.MyHDFSPlainFileReader;
import bdma.labos.hadoop.reader.MyHDFSSequenceFileReader;
import bdma.labos.hadoop.reader.MyReader;
import bdma.labos.hadoop.writer.MyHBaseWriter;
import bdma.labos.hadoop.writer.MyHBaseWriter_C_1;
import bdma.labos.hadoop.writer.MyHBaseWriter_C_2;
import bdma.labos.hadoop.writer.MyHDFSPlainFileWriter;
import bdma.labos.hadoop.writer.MyHDFSSequenceFileWriter;
import bdma.labos.hadoop.writer.MyStandardOutputWriter;
import bdma.labos.hadoop.writer.MyWriter;

public class Main {
	
	private static final String[] names = new String[] {
		"alc",					// Alchol
		"m_acid",				// Malic Acid
		"ash",					// Ash
		"alc_ash",				// Alcalinity of ash
		"mgn",					// Magnesium
		"t_phenols",			// Total phenols
		"flav",					// Flavanoids
		"nonflav_phenols",		// Nonflavanoid phenols
		"proant",				// Proanthocyanins
		"col",					// Color intensity
		"hue",					// Hue
		"od280/od315",			// OD280/OD315 of diluted wines
		"proline"				// Proline
	};
	
	private static final int[][] intervals = new int[][] {
		new int[] {10, 15},
		new int[] {1, 5},
		new int[] {2, 3},
		new int[] {15, 30},
		new int[] {50, 150},
		new int[] {0, 5},
		new int[] {0, 1},
		new int[] {1, 5},
		new int[] {0, 1},
		new int[] {0, 5},
		new int[] {0, 2},
		new int[] {1, 4},
		new int[] {100, 2000}
	};
	
	private static JobMapReduce job;
	private static MyReader input;
	private static MyWriter output;
	private static String file;
	
	private static void read() throws IOException {
		input.open(file);
		String line = input.next();
		while (line != null) {
			if (!line.equals("")) {
				System.out.println(line);
			}
			line = input.next();
		}
		input.close();
	}
	
	private static void write(String[] args) throws NumberFormatException, IOException {
		if (args[2].equals("-instances")) {
			write(true, Integer.parseInt(args[3]));
		}
		else if (args[2].equals("-size")) {
			write(false, (long)(Double.parseDouble(args[3])*1024*1024*1024));
		}
	}
	
	private static void write(boolean inst_mode, long number) throws IOException {
		Generator generator = new Generator(1);
		output.open(file);
		int inst; long size; int bytes;
		for (inst = 1, size = 0; ((inst <= number) & inst_mode) ||
				((size < number) & !inst_mode); inst++, size += bytes) {
			String type = generator.nextType();
			output.put("type", type);
			output.put("region", generator.nextRegion(type));
			for (int j = 0; j < intervals.length; j++) {
				output.put(names[j], generator.nextValue(intervals[j][0], intervals[j][1]));
			}
			bytes = output.flush();
		}
		output.close();
	}

	public static void main(String[] args) {
		try {
			if (args[0].equals("write")) {
				if (args[1].equals("-hdfsPlain")) {
					output = new MyHDFSPlainFileWriter();
					file = args[4];
				}
				else if (args[1].equals("-hdfsSequence")) {
					output = new MyHDFSSequenceFileWriter();
					file = args[4];
				}
				else if (args[1].equals("-standard")) {
					output = new MyStandardOutputWriter();
					file = null;
				}
				else if (args[1].equals("-hbase")) {
					output = new MyHBaseWriter();
					//output = new MyHBaseWriter_C_1();
					//output = new MyHBaseWriter_C_2();
					file = args[4];
				}
				write(args);
			}
			else if (args[0].equals("read")) {
				if (args[1].equals("-hdfsPlain")) {
					input = new MyHDFSPlainFileReader();
				}
				else if (args[1].equals("-hdfsSequence")) {
					input = new MyHDFSSequenceFileReader();
				}
				else if (args[1].equals("-hbase")) {
					input = new MyHBaseReader();
					//input = new MyHBaseReader_C_1();
					//input = new MyHBaseReader_C_2();
				}
				file = args[2];
				read();
			}
			else if (args[0].equals("mapreduce")) {
				if (args[1].equals("-hdfsSequence")) {
					output = new MyHDFSSequenceFileWriter() {
						
						protected SequenceFile.Writer.Option[] options(Path path) {
							return new SequenceFile.Writer.Option[]
							{
									SequenceFile.Writer.file(path),
									SequenceFile.Writer.keyClass(Text.class),
									SequenceFile.Writer.valueClass(Text.class),
									SequenceFile.Writer.blockSize(128*1024),
									SequenceFile.Writer.compression(CompressionType.NONE)
							};
						}
						
					};
					file = args[4];
					write(args);
				}
				else {
					if (args[1].equals("-projection")) {
						job = new ProjectionMapReduce();
					}
					else if (args[1].equals("-selection")) {
						job = new SelectionMapReduce();
					}
					else if (args[1].equals("-groupByAndAgg")) {
						job = new GroupByAndAggMapReduce();
					}
					else if (args[1].equals("-cartesian")) {
						job = new CartesianMapReduce();
					}
					else if (args[1].equals("-join")) {
						job = new JoinMapReduce();
					}
					else if (args[1].equals("-1nn_1")) {
						job = new OneNNMapReduce_1();
					}
					else if (args[1].equals("-1nn_2")) {
						job = new OneNNMapReduce_2();
					}
					job.setInput(args[2]);
					job.setOutput(args[3]);
					job.run();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
