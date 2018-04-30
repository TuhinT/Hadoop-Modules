package com.company;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapreduce.TestNewCombinerGrouping.Combiner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Main {


    public static class Pairs implements Writable{
        private double sum;
        private int count;
        private double square;


        public Pairs(){
        }

        public void readFields(DataInput in) throws IOException{
            sum = in.readDouble();
            count = in.readInt();
            square = in.readDouble();
        }

        public double getSum(){
            return sum;
        }

	public void write(DataOutput dout) throws IOException{
            
            dout.writeDouble(square);
	    dout.writeInt(count);
	    dout.writeDouble(sum);
        }

        

        public double getSquare(){
            return square;
        }

        public void set(double sum, int count){
            this.sum = sum;
            this.count = count;
        }

        public void setSquare(double square){
            this.square = square;
        }
	public int getCount(){
            return count;
        }


        @Override
        public String toString() {
            // TODO Auto-generated method stub
            return ""+sum+ "    " + square ;
        }

    }



    public static class Map extends Mapper<LongWritable, Text, Text,Pairs>{
        //private final static IntWritable one = new IntWritable(1);

        
        private Pairs value = new Pairs();
	private Text word = new Text();

        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException
        {
            String line = values.toString();
            int k = 1;

            value.set(Double.parseDouble(line),1);
            System.out.println("number "+line);

            word.set(new Text(""));
            context.write(word,value);

        }

    }






    public static class Combine extends Reducer<Text,Pairs,Text,Pairs> {
        private Pairs value = new Pairs();

        public void reduce(Text key, Iterable<Pairs> values,Context context) throws IOException, InterruptedException {


            
            double square = 0;double sum = 0;
            int count = 0;

            for (Pairs pair:values){
                sum += pair.getSum();
                count += pair.getCount();

                square+=Math.pow(pair.getSum(),2);

                System.out.println("Combiner "+sum + " "+ count );
            }


            value.set(sum, count);
            value.setSquare(square);

            context.write(key, value);


        }

    }



    public static class Reduce extends Reducer<Text,Pairs,Text,Pairs> {
        //private IntWritable result = new IntWritable();
        private Pairs value = new Pairs();

        public void reduce(Text key, Iterable<Pairs> values,Context context) throws IOException, InterruptedException {
            double sum = 0; // initialize the sum for each keyword
            int count = 0;
            double square =0;

            for (Pairs pair:values){
                sum += pair.getSum();
                count += pair.getCount();

                square+=pair.getSquare();

                System.out.println("Reducer "+sum + " "+ count + " "+ square);
            }

            double average = sum/count;
            double variance = (square/count) - Math.pow(average,2);


            System.out.println("avg "+average);
            System.out.println("variance "+ variance);
            //result.set(sum);
            value.set(average, count);
            value.setSquare(variance);

            context.write(key,value);// create a pair <keyword, number of occurences>
        }
    }


    // Driver program
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: WordCount <in> <out>");
            System.exit(2);
        }

        // create a job with name "MeanVariance"
        Job job = Job.getInstance(conf, "MeanVariance");
        job.setJarByClass(Main.class);
        
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        
        // set output key type
        job.setOutputKeyClass(Text.class);

        // set output value type
        job.setOutputValueClass(Pairs.class);

        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
