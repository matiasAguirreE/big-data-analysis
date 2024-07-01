package org.mdp.hadoop.cli;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountAvailableDocksByMonth {

    public static class cadbmMapper extends Mapper<Object, Text, Text, Text> {
        private static final int STATION_ID_INDEX = 0;
        private static final int NUM_BIKES_AVAILABLE_INDEX = 1;
        private static final int NUM_DOCKS_AVAILABLE_INDEX = 4;

        private boolean isFirstLine = true;

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (isFirstLine) {
                isFirstLine = false;
                return;
            }
            String[] fields = line.split(",");
            
            if (fields.length > NUM_DOCKS_AVAILABLE_INDEX && 
                isValid(fields[STATION_ID_INDEX]) && 
                isValid(fields[NUM_BIKES_AVAILABLE_INDEX]) && 
                isValid(fields[NUM_DOCKS_AVAILABLE_INDEX])) {

                String station_Id = fields[STATION_ID_INDEX];
                int stationId = Integer.parseInt(fields[STATION_ID_INDEX]);
                int numBikesAvailable = Integer.parseInt(fields[NUM_BIKES_AVAILABLE_INDEX]);
                int numDocksAvailable = Integer.parseInt(fields[NUM_DOCKS_AVAILABLE_INDEX]);
                String outputValue =  stationId + "," + numBikesAvailable + "," + numDocksAvailable;

                context.write(new Text(station_Id), new Text(outputValue));
            }
        }

        private boolean isValid(String field) {
            return field != null && !field.isEmpty() && !field.equals("NA");
        }
    }

    public static class cadbmReducer extends Reducer<Text, Text, Text, Text> {
        private int totalBikesAvailable = 0;
        private int totalDocksAvailable = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> uniqueRowsSet = new HashSet<>();

            for (Text value : values) {
                String[] fields = value.toString().split(",");
                if (fields.length > 2 && 
                    isValid(fields[0]) && 
                    isValid(fields[1]) && 
                    isValid(fields[2])) {
                    
                    String stationId = fields[0];

                    if (!uniqueRowsSet.contains(stationId)) {
                        uniqueRowsSet.add(stationId);
                        int numBikesAvailable = Integer.parseInt(fields[1]);
                        int numDocksAvailable = Integer.parseInt(fields[2]);

                        totalBikesAvailable += numBikesAvailable;
                        totalDocksAvailable += numDocksAvailable;
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            double percentageDocksAvailable = (double) totalDocksAvailable / (totalBikesAvailable + totalDocksAvailable) * 100;
            String outputValue = String.format("%.2f", percentageDocksAvailable) + " %";
            context.write(new Text("mes"), new Text(outputValue));
        }

        private boolean isValid(String field) {
            return field != null && !field.isEmpty() && !field.equals("NA");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CountAvailableDocksByMonth <input path> <output path>");
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

        job.setMapperClass(cadbmMapper.class);
        job.setReducerClass(cadbmReducer.class);

        job.setJarByClass(CountAvailableDocksByMonth.class);
        job.waitForCompletion(true);
    }
}