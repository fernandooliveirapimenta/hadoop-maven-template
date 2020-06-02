package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

public class ExemploIGTI extends Configured implements Tool {

    private static Logger logger = Logger.getLogger(ExemploIGTI.class);
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(
                new Configuration(),
                new ExemploIGTI(),
                args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        JobConf conf = new JobConf(getConf(), ExemploIGTI.class);
        conf.setJobName("FernandoJob");
        Job job = Job.getInstance(conf, "FernandoJob");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(MapIGTI.class);
        conf.setReducerClass(ReduceIGTI.class);
        conf.setCombinerClass(ReduceIGTI.class);
        return 0;
    }

    public static class MapIGTI extends MapReduceBase
            implements Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable longWritable,
                        Text text,
                        OutputCollector<Text, Text> outputCollector,
                        Reporter reporter) throws IOException {

        }
    }

    public static class ReduceIGTI extends MapReduceBase
    implements  Reducer<Text, Text, Text, Text> {

        public void reduce(Text text,
                           Iterator<Text> iterator,
                           OutputCollector<Text, Text> outputCollector,
                           Reporter reporter) throws IOException {

        }
    }
}
