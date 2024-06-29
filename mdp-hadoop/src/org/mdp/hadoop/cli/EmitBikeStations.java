package org.mdp.hadoop.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

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
 * Java class to emit pairs of stations with similar bike availability
 * based on their current status.
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
public class EmitBikeStations {
    
    public static String IN_SERVICE = "IN_SERVICE";
    
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
    public static class EmitBikeStationsMapper extends Mapper<Object, Text, Text, Text>{

        @Override
        public void map(Object keyIn, Text valueIn,
                Context output)
                        throws IOException, InterruptedException {
            String[] split = valueIn.toString().split(",");
            if (split[10].equals(IN_SERVICE)) {
                Text valueOut = new Text(split[0]); // station_id
                Text keyOut = new Text(split[10]); // status
                output.write(keyOut, valueOut);
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
    public static class EmitBikeStationsReducer extends 
         Reducer<Text, Text, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values,
                Context output) throws IOException, InterruptedException {
            ArrayList<String> list = new ArrayList<String>();
            for (Text valueIn: values) {
                list.add(valueIn.toString());
            }
            sortAndDeduplicate(list);
            for (int i=0; i<list.size(); i++) {
                for (int j=i+1; j<list.size(); j++) {
                     Text keyOut = new Text(list.get(i)+"##"+list.get(j));
                     IntWritable valueOut = new IntWritable(1);
                     output.write(keyOut, valueOut);
                }
            }
        }

        private static void sortAndDeduplicate(ArrayList<String> list) {
            Collections.sort(list);
            for(int i=0; i<list.size()-1; i++) {
                if(list.get(i).equals(list.get(i+1))) {
                    list.remove(i+1);
                    i--;
                }
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
            System.err.println("Usage: "+EmitBikeStations.class.getName()+" <in> <out>");
            System.exit(2);
        }
        String inputLocation = otherArgs[0];
        String outputLocation = otherArgs[1];
        
        Job job = Job.getInstance(new Configuration());
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapperClass(EmitBikeStationsMapper.class);
        job.setReducerClass(EmitBikeStationsReducer.class);
        
        FileInputFormat.setInputPaths(job, new Path(inputLocation));
        FileOutputFormat.setOutputPath(job, new Path(outputLocation));
        
        job.setJarByClass(EmitBikeStations.class);
        job.waitForCompletion(true);
    }    
}