package com.company;

import java.awt.List;
import java.io.IOException;
import java.util.ArrayList;

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
import org.apache.hadoop.util.GenericOptionsParser;

public class Main{
    static class MatrixMultiplicationMapper extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("********");
            String[] data = value.toString().split(",");
            String pair = new String();
            String colOrRow = new String();
            if(data[0].equals("A")) {
                colOrRow  = data[2];
                pair = "A" + ","+ data[1].trim()+ ","+ data[3].trim();
                System.out.println("********"+pair);
            }else if(data[0].equals("B")) {
                colOrRow = data[1];
                pair = "B" + ","+ data[2].trim()+ ","+ data[3].trim();
                System.out.println("********"+pair);
            }
            context.write(new Text(colOrRow), new Text(pair));
        }
    }
    static class MatrixMultiplicationReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable <Text> listPairs, Context context) throws IOException, InterruptedException {
            ArrayList<String> al = new ArrayList<String>();
            for(Text pairs: listPairs) {
                al.add(pairs.toString());
            }

            for(int i=0;i<al.size();i++) {
                String[] v1 = al.get(i).split(",");
                System.out.println("****herev1***"+al.get(i).toString());
                if(v1[0].equals("A")) {

                    String row = v1[1];String rowVal = v1[2];
                    for(int j=0;j<al.size();j++) {
                        String pair = new String();
                        String[] v2 = al.get(j).split(",");
                        String col = v2[1]; String colVal = v2[2];
                        if(v2[0].equals("B")) {
                            System.out.println("****herev2***"+al.get(j).toString());
                            pair = row+"_"+col;
                            int mul = (Integer.parseInt(rowVal)*Integer.parseInt(colVal));
                            String value = String.valueOf(mul);
                            context.write(new Text(pair),new Text(value));
                        }
                    }
                }
            }





        }


    }
    static class MatrixMultiplicationMapper1 extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            IntWritable mapValue = new IntWritable();
            if(data.length==2) {
                String mapKey = data[0];
                String v = data[1];
                int mValue = Integer.parseInt(v);
                mapValue.set(mValue);
                context.write(new Text(mapKey), mapValue);
            }
        }
    }

    static class MatrixMultplicationReducer1 extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text mapKey, Iterable <IntWritable> listMValues, Context context) throws IOException, InterruptedException {
            int v=0;IntWritable mul = new IntWritable();
            for(IntWritable value: listMValues) {
                v = v + value.get();
            }
            mul.set(v);
            context.write(mapKey,mul);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.out.println("Usage: hadoop jar <jarFile> ClassName <soc-LiveJournal1Adj.txt path> <output folder path1 for mapreduce1> <Final Output Path>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Matrix multiplication");
        job.setJarByClass(Main.class);
        job.setMapperClass(MatrixMultiplicationMapper.class);
        job.setReducerClass(MatrixMultiplicationReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //using job chaining to send first output to the second mapper.
        if (job.waitForCompletion(true)) {
            Configuration conf1 = new Configuration();
            Job job2 = new Job(conf1, "Matrix multiplication 2");
            job2.setJarByClass(Main.class);
            job2.setMapperClass(MatrixMultiplicationMapper1.class);
            job2.setReducerClass(MatrixMultplicationReducer1.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(IntWritable.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);
            //first mapreduce output as input for second mapreduce.
            FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }


}