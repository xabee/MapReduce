package bdma.labos.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GroupByAndAggMapReduce extends JobMapReduce {
	
	public static class GroupByAndAggMapper extends Mapper<Text, Text, Text, DoubleWritable> {
		
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			// Obtain the parameters sent during the configuration of the job
			String groupBy = context.getConfiguration().getStrings("groupBy")[0];
			String agg = context.getConfiguration().getStrings("agg")[0];
			
			// Since the value is a CSV, just get the lines split by commas
			String[] arrayValues = value.toString().split(",");
			String groupByValue = Utils.getAttribute(arrayValues, groupBy);
			double aggValue = Double.parseDouble(Utils.getAttribute(arrayValues, agg));
			
			// Do the group by and emit it
			context.write(new Text(groupByValue), new DoubleWritable(aggValue));
		}
		
	}
	
	public static class GroupByAndAggReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			for (DoubleWritable value : values) {
				sum += value.get();
			}
			context.write(key, new DoubleWritable(sum));
		}
		
	}
	
	public GroupByAndAggMapReduce() {
		this.input = null;
		this.output = null;
	}
	
	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		
		// Define the new job and the name it will be given
		Job job = Job.getInstance(configuration, "GroupByAndAgg");
		job.setJarByClass(GroupByAndAggMapReduce.class);
		
		// Set the mapper class it must use
		job.setMapperClass(GroupByAndAggMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		// Set the combiner class it muse use
		job.setCombinerClass(GroupByAndAggReducer.class);
		
		// Set the reducer class it must use
		job.setReducerClass(GroupByAndAggReducer.class);
		
		// The output will be Text
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    
	    // The files the job will read from/write to
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    FileInputFormat.addInputPath(job, new Path(this.input));
	    FileOutputFormat.setOutputPath(job, new Path(this.output));
	    
	    // These are the parameters that we are sending to the job
	    job.getConfiguration().setStrings("groupBy", "type");
	    job.getConfiguration().setStrings("agg", "col");
	    
	    // Let's run it!
	    return job.waitForCompletion(true);
	}
	
}
