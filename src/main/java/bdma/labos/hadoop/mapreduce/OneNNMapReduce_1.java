package bdma.labos.hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OneNNMapReduce_1 extends JobMapReduce {
	
	private static int N = 100;
	
	public static class OneNNMapper_1 extends Mapper<LongWritable, Text, IntWritable, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (1==1/* CODE HERE */) {
				// Obtain the parameters sent during the configuration of the job
				String train = context.getConfiguration().getStrings("train")[0];
				
				// Since the value is a CSV, just get the lines split by commas
				String[] values = value.toString().split(",");
				String isTrain = Utils_1NN.getAttribute(values, train);
				
				// Do the cartesian product and emit it
				if (isTrain.equals("1")) {
					// CODE HERE
				}
				else {
					// CODE HERE
				}
			}
		}
		
	}
	
	public static class OneNNReducer_1 extends Reducer<IntWritable, Text, NullWritable, Text> {
		
		private static ArrayList<String> predictors = Utils_1NN.getPredictors();
		
		private double distance(String a, String b) {
			String[] arrayA = a.split(",");
			String[] arrayB = b.split(",");
			double distance = 0;
			for (int i = 0; i < predictors.size(); i++) {
				String valueA = Utils_1NN.getAttribute(arrayA, predictors.get(i));
				String valueB = Utils_1NN.getAttribute(arrayB, predictors.get(i));
				
				// CODE HERE
			}
			return 1/* CODE HERE */;
		}
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String train = context.getConfiguration().getStrings("train")[0];
			String diagnosis = context.getConfiguration().getStrings("diagnosis")[0];
			
			// Let's separate between the external and internal sets
			ArrayList<String> arrayTrain = new ArrayList<String>();
			ArrayList<String> arrayTest = new ArrayList<String>();
			for (Text value : values) {
				String[] arrayValue = value.toString().split(",");
				String isTrain = Utils_1NN.getAttribute(arrayValue, train);
				// If it is external, let's add it as it
				if (isTrain.equals("1")) {
					// CODE HERE
				}
				// If it is internal, let's add it as it
				else {
					// CODE HERE
				}
			}
			// Finally, let's iterate over both external and internal sets
			// and computing distances
			for (String rowTest : arrayTest) {
				// Initialization of distance and class
				double min_distance = Double.MAX_VALUE;
				String min_diagnosis = null;
				for (String rowTrain : arrayTrain) {
					// CODE HERE
				}
				// Output the results in form of PREDICTION_TRUTH
				String truth = Utils_1NN.getAttribute(rowTest.split(","), diagnosis);
				context.write(NullWritable.get(), new Text(/* CODE HERE */));
			}
		}
		
	}
	
	public OneNNMapReduce_1() {
		this.input = null;
		this.output = null;
	}
	
	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		
		// Define the new job and the name it will be given
		Job job = Job.getInstance(configuration, "1NN_1");
		job.setJarByClass(OneNNMapReduce_1.class);
		
		// Set the mapper class it must use
		job.setMapperClass(OneNNMapper_1.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		// Set the reducer class it must use
		job.setReducerClass(OneNNReducer_1.class);
		
		// The output will be Text
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    // These are the parameters that we are sending to the job
	    job.getConfiguration().setStrings("train", "train");
	    job.getConfiguration().setStrings("diagnosis", "diagnosis");
	    
	    // The files the job will read from/write to
	    FileInputFormat.addInputPath(job, new Path(this.input));
	    FileOutputFormat.setOutputPath(job, new Path(this.output));
	    
	    // Let's run it!
	    return job.waitForCompletion(true);
	}
	
}
