package HFiveOne;

import java.util.StringTokenizer;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib. input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


//main class that contain main method 
public class HFiveTwo
{
	
// mapper class 	
public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
{
	
	
	//overridin map method of mapper class
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		
		String[] splits = value.toString().split("\\|");  //spilts the file line on the baises of | 
		Text word =new Text(splits[0].toString()); 			//fetching the company name from the splits array
		

		context.write(word, new IntWritable(1));			//output of the mapper class
			
	
		
	}
}


//reduceer class
public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
{
	//overriddin reduce method of the reducer class which will iterate through every value
	//and making the count using sum variable  
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		
		int sum = 0;
		while(values.iterator().hasNext())
		{
			sum+=values.iterator().next().get();
		}
		context.write(key, new IntWritable(sum));		//ouput of the reducer class
	}
}

//main method
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 

	{

Configuration conf = new Configuration();
Job job = new Job(conf,"MapReduce-1");
job.setJarByClass(HFiveTwo.class);

job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);

job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
job.setCombinerClass(Reduce.class);    //adding the combiner class which is same as reduceer class

job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);

FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

job.waitForCompletion(true);
	}

}

