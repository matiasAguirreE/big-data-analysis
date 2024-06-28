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
 * Java class to sort the counts of star pairs
 * 
 * @author Aidan
 */
public class SortCoStarCounts {

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
	 * MapKey will be IntWritable: the count for the star pair
	 * 
	 * MapValue will be Text: the star pair
	 * 
	 * @author Aidan
	 *
	 */
	public static class SortCoStarsMapper extends 
		Mapper<Object, Text, IntWritable, Text>{

		/**
		 * @throws InterruptedException 
		 * 
		 * This is the map method that you're going to write. :)
		 * 
		 */
		@Override
		public void map(Object key, Text value, Context output)
						throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			output.write(new DescendingIntWritable(Integer.parseInt(split[1])),new Text(split[0]));
		}
	}

	/**
	 * This is the Reducer Class.
	 * 
	 * This collects sets of key-value pairs with the same key on one machine. 
	 * 
	 * Remember that the generic is Reducer<MapKey, MapValue, OutputKey, OutputValue>
	 * 
	 * 		 
	 * MapKey will be IntWritable: the star pair counts
	 * 
	 * MapValue will be Text: the star pair
	 * 
	 * OutputKey will be Text: the star pair
	 * 
	 * OutputValue will be IntWritable: the star pair counts
	 * 
	 * @author Aidan
	 *
	 */
	public static class SortCoStarsReducer 
	       extends Reducer<DescendingIntWritable, Text, Text, IntWritable> {

		/**
		 * @throws InterruptedException 
		 * 
		 */
		@Override
		public void reduce(DescendingIntWritable key, Iterable<Text> values,
				Context output) throws IOException, InterruptedException {
			for(Text value:values) {
				output.write(value,key);
			}
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
			System.err.println("Usage: "+SortCoStarCounts.class.getName()+" <in> <out>");
			System.exit(2);
		}
		String inputLocation = otherArgs[0];
		String outputLocation = otherArgs[1];

		Job job = Job.getInstance(new Configuration());
		job.setMapOutputKeyClass(DescendingIntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(SortCoStarsMapper.class);
		job.setReducerClass(SortCoStarsReducer.class);

		FileInputFormat.setInputPaths(job, new Path(inputLocation));
		FileOutputFormat.setOutputPath(job, new Path(outputLocation));

		job.setJarByClass(SortCoStarCounts.class);
		job.waitForCompletion(true);
	}	
	
	public static class DescendingIntWritable extends IntWritable{
		
		public DescendingIntWritable(){}
		
		public DescendingIntWritable(int val){
			super(val);
		}
		
		public int compareTo(IntWritable o) {
			return -super.compareTo(o);
		}
	}
}
