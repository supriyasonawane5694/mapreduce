import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfitMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			String[] record = value.toString().split(";");
			
			//String prod = record[5];
			String catg = record[4];
			long cost = Long.valueOf(record[7]);
			long sales = Long.valueOf(record[8]);
			long totprof = (sales - cost);
			//context.write(new Text(prod), new LongWritable(totprof));
			context.write(new Text(catg), new LongWritable(totprof));
			
		} catch (IndexOutOfBoundsException e) {
		} catch (ArithmeticException e1) {
		}
	}

}
