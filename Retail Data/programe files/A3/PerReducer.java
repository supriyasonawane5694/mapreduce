import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PerReducer extends Reducer<Text, Text, Text, Text>
{
    
    
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
    {
    	float tot_sales=0;
        float tot_cost=0;
        float profit=0;
		
         for (Text val : values)
         {    
        	String[] token = val.toString().split(",");
        	tot_cost+=Float.parseFloat(token[0]);
        	tot_sales+=Float.parseFloat(token[1]);
         }
         
      profit = (((tot_sales-tot_cost)/tot_cost)*100);	
      String myMaxValue = String.format("%f",profit);
      //context.write(key, result);
      context.write(key, new Text(myMaxValue));
      
    }
}
