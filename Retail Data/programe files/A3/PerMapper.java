import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PerMapper extends Mapper<LongWritable, Text, Text, Text>
{
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		try {
			String[] rec = value.toString().split(";");
			
			//String prod = rec[5];
			String catg = rec[4];
			String cost = rec[7];
			String sales = rec[8];
			String perprof = cost + ',' + sales;
			context.write(new Text(catg), new Text(perprof));			
		} catch (IndexOutOfBoundsException e) {
		} catch (ArithmeticException e1) {
		}
	}
}
