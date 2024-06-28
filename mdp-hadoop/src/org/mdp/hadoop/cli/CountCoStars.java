package org.mdp.hadoop.cli;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Java class to add the partial counts of costars together
 * 
 * @author Aidan
 */
public class CountCoStars {

	/**
	 * This is the Mapper Class. This sends key-value pairs to different machines
	 * based on the key.
	 * 
	 * Remember that the generic is Mapper<InputKey, InputValue, MapKey, MapValue>
	 * 
	 * InputKey we don't care about (a LongWritable will be passed as the input
	 * file offset, but we don't care; we can also set as Object)
	 * 
	 * InputKey will be Text: a line of the file
	 * 
	 * MapKey will be Text: the star pair
	 * 
	 * MapValue will be IntWritable: the star pair count
	 * 
	 * @author Aidan
	 *
	 */
	public static class CountCoStarsMapper extends Mapper<Object, Text, Text, IntWritable>{

		/**
		 * @throws InterruptedException 
		 */
		@Override
		public void map(Object key, Text value, Context context)
						throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			context.write(new Text(split[0]),new IntWritable(Integer.parseInt(split[1])));
		}
	}

	/**
	 * This is the Reducer Class.
	 * 
	 * This collects sets of key-value pairs with the same key on one machine. 
	 * 
	 * Remember that the generic is Reducer<MapKey, MapValue, OutputKey, OutputValue>
	 * 
	 * @author Aidan
	 *
	 */
	public static class CountCoStarsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		/**
		 * @throws InterruptedException 
		 * 
		 * 
		 * MapKey will be Text: the star pair
		 * 
		 * MapValue will be IntWritable: the raw star pair counts
		 * 
		 * OutputKey will be Text: the star pair
		 * 
		 * OutputValue will be IntWritable: the total star pair count
		 */
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	/**
	 * Main method that sets up and runs the job
	 * 
	 * @param args First argument is input, second is output
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: "+CountCoStars.class.getName()+" <in> <out>");
			System.exit(2);
		}
		String inputLocation = otherArgs[0];
		String outputLocation = otherArgs[1];
		
		Job job = Job.getInstance(new Configuration());
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(CountCoStarsMapper.class);
		job.setCombinerClass(CountCoStarsReducer.class);
		job.setReducerClass(CountCoStarsReducer.class);
		
		FileInputFormat.setInputPaths(job, new Path(inputLocation));
		FileOutputFormat.setOutputPath(job, new Path(outputLocation));
		
		job.setJarByClass(CountCoStars.class);
		job.waitForCompletion(true);
	}	
}
