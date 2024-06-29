package org.mdp.hadoop.cli;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Java class to emit percentage of bikes available.
 *
 * The input CSV file contains the following (String[] split = inputLine.split(",");):
 		 * split[0] is the station id
		 * split[1] is the num of bikes available
		 * split[2] is the num of mechanical bikes available
		 * split[3] is the num of ebikes available
		 * split[4] is the num of docks available
		 * split[5] is a number that say if the station is properly installed
		 * split[6] is a number that say if the station is providing bikes correctly
		 * split[7] is a number that say if the station is docking bikes correctly
		 * split[8] is the timestamp of the station information
		 * split[9] is a number that say if the station has electric bike charging capacity
		 * split[10] is the status of the station
		 * split[11] is the timestamp where the data was last updated
		 * split[12] is the time to live of the data
 */
public class BikesJob {
    private static final int STATION_ID_INDEX = 0;
    private static final int NUM_BIKES_AVAILABLE_INDEX = 1;
    private static final int NUM_DOCKS_AVAILABLE_INDEX = 4;

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
     * MapKey will be Text: the station status
     * 
     * MapValue will be Text: the station id
     * 
     */
    public static class BikesMapper extends Mapper<LongWritable, Text, Text, Text> {
        private boolean isFirstLine = true;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            if (isFirstLine) {
                isFirstLine = false;
                return;
            }
	        if (fields.length > NUM_DOCKS_AVAILABLE_INDEX) {
	        	String stationId = fields[STATION_ID_INDEX];
	            int numBikesAvailable = Integer.parseInt(fields[NUM_BIKES_AVAILABLE_INDEX]);
	            int numDocksAvailable = Integer.parseInt(fields[NUM_DOCKS_AVAILABLE_INDEX]);
	            int sum = numBikesAvailable + numDocksAvailable;
	            double percentage = (double) numBikesAvailable / sum * 100;
	            String outputValue = numBikesAvailable + "," + numDocksAvailable + "," + sum + "," + String.format("%.2f", percentage) + " %";
	            context.write(new Text(stationId), new Text(outputValue));
	                
	        }
        }
    }

    /**
     * This is the Reducer Class.
     * 
     * This collects sets of key-value pairs with the same key on one machine. 
     * 
     * Remember that the generic is Reducer<MapKey, MapValue, OutputKey, OutputValue>
     * 
     * MapKey will be Text: the station status
     * 
     * MapValue will be Text: the station id
     * 
     * OutputKey will be Text: a station pairing
     * 
     * OutputValue will be IntWritable: the initial count
     * 
     */
    public static class BikesReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (values.iterator().hasNext()) {
                context.write(key, values.iterator().next());
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
        if (args.length != 2) {
            System.err.println("Usage: BikesJob <input path> <output path>");
            System.exit(-1);
        }
        Job job = Job.getInstance(new Configuration());
	    
        String inputLocation = args[0];
		String outputLocation = args[1];
	    FileInputFormat.setInputPaths(job, new Path(inputLocation));
	    FileOutputFormat.setOutputPath(job, new Path(outputLocation));
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setMapperClass(BikesMapper.class);
	    job.setReducerClass(BikesReducer.class);
	     
	    job.setJarByClass(BikesJob.class);
		job.waitForCompletion(true);
    }
}
