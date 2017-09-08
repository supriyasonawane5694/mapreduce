import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class TotalGrProfit 
{
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out
					.printf("Usage: TotalGrossProfit <input dir> <output dir>\n");
			System.exit(-1);
		}

		
		Configuration conf = new Configuration();
		//conf.set("","")
		Job job = Job.getInstance(conf, "Total_Profit_Product");

		job.setJarByClass(TotalGrProfit.class);

		//job.setJobName("NYSE Stock");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(ProfitMapper.class);
		//job.setCombinerClass(NYSEReducer.class);
		job.setReducerClass(ProfitReducer.class);
		//job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(LongWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}
}
