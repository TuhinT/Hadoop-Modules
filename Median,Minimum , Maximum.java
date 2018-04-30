package com.company;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

public class Main extends Configured implements Tool {

    public static class Map extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Text key1 = new Text("" + 1);

            context.write( key1, value);
        }

    }


    public static class Reduce extends Reducer<Text,Text,Text,Text> {
        
        ArrayList<Integer> list = new ArrayList<Integer>();
	int median = 0;
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {


            for(Text v: values){
                list.add(Integer.parseInt(v.toString()));
            }

            Collections.sort(list);

            int size = list.size();

            if(size % 2 == 0)
	    {
                median = list.get(size / 2);
            }
            else{
                median = list.get(  ((size + 1)/2)  - 1);
            }

            context.write(new Text( "Min " + list.get(0)+ " Max " + list.get(size - 1) + " Median " + median ), new Text(""));
        }
    }


    public int run(String[] args) throws Exception{
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "problem 2");
        job.setJarByClass(Main.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);

        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setNumReduceTasks(1);

        job.setMapperClass(Main.Map.class);
        job.setReducerClass(Main.Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true)?0:1);

        return 0;
    }
	//this is what is driving the program
    public static void main(String[] args) throws Exception
   {
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);
    }
}
