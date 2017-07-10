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



public class HFiveOne {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			
			String[] splits = value.toString().split("\\|");
			
			Text word =new Text(splits[0].toString());
			
		

			context.write(word, new IntWritable(1));
				
		
			
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		@Override
		

		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			while(values.iterator().hasNext())
			{
				sum+=values.iterator().next().get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static class Partition extends Partitioner<Text,IntWritable>{


		@Override
		public int getPartition(Text key, IntWritable value, int arg2) {
			
			char keyele = key.toString().charAt(0);
			int ascii = (int)keyele;
			
			if(65 <=ascii && ascii <= 70 ){
				return 0;
			}
			else  if(97 <=ascii && ascii <= 102){
				return 0;
			}
			else if(71 <=ascii && ascii <= 76){
				
				return 1;
			}
			else if(103 <=ascii && ascii <= 108){
				return 1;
			}
			else if(77 <=ascii && ascii <= 82){
				return 2;
			}
			else if(109 <=ascii && ascii <= 114){
				return 2;
			}
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
		job.setPartitionerClass(Partition.class);
		
		job.setNumReduceTasks(4);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}

}

