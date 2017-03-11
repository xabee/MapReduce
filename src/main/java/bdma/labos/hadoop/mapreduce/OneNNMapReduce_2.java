package bdma.labos.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OneNNMapReduce_2 extends JobMapReduce {
	
	public static class OneNNMapper_2 extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// CODE HERE
		}
		
	}
	
	public static class OneNNReducer_2 extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			// CODE HERE
		}
		
	}
	
	public OneNNMapReduce_2() {
		this.input = null;
		this.output = null;
	}
	
	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		
		// Define the new job and the name it will be given
		Job job = Job.getInstance(configuration, "1NN_2");
		job.setJarByClass(OneNNMapReduce_2.class);
		
		// Set the mapper class it must use
		job.setMapperClass(OneNNMapper_2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// Set a combiner
		job.setCombinerClass(OneNNReducer_2.class);
		
		// Set the reducer class it must use
		job.setReducerClass(OneNNReducer_2.class);
		
		// The output will be Text
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    // The files the job will read from/write to
	    FileInputFormat.addInputPath(job, new Path(this.input));
	    FileOutputFormat.setOutputPath(job, new Path(this.output));
	    
	    // Let's run it!
	    return job.waitForCompletion(true);
	}
	
}
