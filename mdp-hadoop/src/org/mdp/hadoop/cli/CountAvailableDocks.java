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

public class CountAvailableDocks {

    public static class BikesMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final int STATION_ID_INDEX = 0;
        private static final int NUM_BIKES_AVAILABLE_INDEX = 1;
        private static final int NUM_DOCKS_AVAILABLE_INDEX = 4;
        private boolean isFirstLine = true;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if (isFirstLine) {
                isFirstLine = false;
                context.write(new Text("STATION ID"), new Text("% Docks Available"));
                return;
            }

            String stationId = fields[STATION_ID_INDEX];
            int numBikesAvailable = Integer.parseInt(fields[NUM_BIKES_AVAILABLE_INDEX]);
            int numDocksAvailable = Integer.parseInt(fields[NUM_DOCKS_AVAILABLE_INDEX]);
            int totalDocks = numBikesAvailable + numDocksAvailable;
            double percentageDocksAvailable = (double) numDocksAvailable / totalDocks * 100;
            String outputValue = String.format("%.2f", percentageDocksAvailable) + " %";
            context.write(new Text(stationId), new Text(outputValue));

        }
    }

    public static class BikesReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            if (values.iterator().hasNext())
                context.write(key, values.iterator().next());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: countAvailableDocks <input path> <output path>");
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

        job.setJarByClass(CountAvailableDocks.class);
        job.waitForCompletion(true);
    }
}
