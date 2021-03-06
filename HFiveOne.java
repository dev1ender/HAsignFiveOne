package HFiveOne;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


//main class that contain the main method
public class HFiveOne {
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
	
	//partition class which is custom partitioner extending partition class
	//used to decide which key goes to which reducer
	public static class Partition extends Partitioner<Text,IntWritable>{


		@Override
		public int getPartition(Text key, IntWritable value, int arg2) {

			char keyele = key.toString().charAt(0);
			int ascii = (int)keyele;

			//for captial A-F 
			if(65 <=ascii && ascii <= 70 ){
				return 0;
			}
			
			//for small a-f
			else  if(97 <=ascii && ascii <= 102){
				return 0;
			}
			//for captial G-L 
			else if(71 <=ascii && ascii <= 76){

				return 1;
			}
			//for small g-l
			else if(103 <=ascii && ascii <= 108){
				return 1;
			}
			//for captial M-R 
			else if(77 <=ascii && ascii <= 82){
				return 2;
			}
			//for small m-r
			else if(109 <=ascii && ascii <= 114){
				return 2;
				
			}
			//remaing goes to this
			else{
				return 3;

			}




		}

	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf,"MapReduce-2");
		job.setJarByClass(HFiveOne.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setPartitionerClass(Partition.class);		//partition class added 
		
		job.setNumReduceTasks(4);						//no of reducer set 

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}

}

