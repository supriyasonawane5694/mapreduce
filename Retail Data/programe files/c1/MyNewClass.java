import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;


public class MyNewClass extends Configured implements Tool
{
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {
	         try{
	            String[] str = value.toString().split(";");
	            String prod=str[5];
	            context.write(new Text(prod), new Text(value));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	public static class CaderPartitioner extends
	   Partitioner < Text, Text >
	   {
	      @Override
	      public int getPartition(Text key, Text value, int numReduceTasks)
	      {
	         String[] str = value.toString().split(";");
	         String age = str[2];	         
	         
	         if(age.equalsIgnoreCase("A"))
	         {
	            return 0;
	         }
	         else if(age.equalsIgnoreCase("B"))
	         {
	            return 1;
	         }
	         else if(age.equalsIgnoreCase("C"))
	         {
	            return 2;
	         }
	         else if(age.equalsIgnoreCase("D"))
	         {
	            return 3;
	         }
	         else if(age.equalsIgnoreCase("E"))
	         {
	            return 4;
	         }
	         else if(age.equalsIgnoreCase("F"))
	         {
	            return 5;
	         }
	         else if(age.equalsIgnoreCase("G"))
	         {
	            return 6;
	         }
	         else if(age.equalsIgnoreCase("H"))
	         {
	            return 7;
	         }
	         else if(age.equalsIgnoreCase("I"))
	         {
	            return 8;
	         }
	         else
	         {
	        	 return 9;
	         }
	      }
	   }
	
	public static class ReduceClass extends Reducer<Text,Text,Text,LongWritable>
	   {
	      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
	      {
	    	  long cost=0;
		      long sales=0;
		      long profit=0;
		      long sum=0;
		         for (Text val : values)
		         {   
		        	 String[] str = val.toString().split(";"); 
		        	 cost=Long.parseLong(str[7]);
		        	 sales=Long.parseLong(str[8]);
		        	 profit = sales-cost;
		        	 if(profit > 0)
		        	 {
			        	sum+=profit; 
		        	 }
		        	
		         }
		         
		         context.write(key, new LongWritable(sum));
	      }
	   }
	
	public int run(String[] arg) throws Exception
	   {
		
		   
		  Configuration conf = new Configuration();
		  Job job = Job.getInstance(conf);
		  job.setJarByClass(MyNewClass.class);
		  job.setJobName("Top Salaried Employees");
	      FileInputFormat.setInputPaths(job, new Path(arg[0]));
	      FileOutputFormat.setOutputPath(job,new Path(arg[1]));
			
	      job.setMapperClass(MapClass.class);
			
	      job.setMapOutputKeyClass(Text.class);
	      job.setMapOutputValueClass(Text.class);
	      
	      //set partitioner statement
			
	      job.setPartitionerClass(CaderPartitioner.class);
	      //job.setReducerClass(ReduceClass.class);
	      job.setNumReduceTasks(10);
	      job.setInputFormatClass(TextInputFormat.class);
			
	      job.setOutputFormatClass(TextOutputFormat.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(LongWritable.class);
			
	      System.exit(job.waitForCompletion(true)? 0 : 1);
	      return 0;
	   }
	   
	   public static void main(String ar[]) throws Exception
	   {
	      ToolRunner.run(new Configuration(), new MyNewClass(),ar);
	      System.exit(0);
	   }
}
