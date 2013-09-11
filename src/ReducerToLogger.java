import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReducerToLogger extends Reducer<Text, IntWritable, Text, IntWritable>
    {
    	static final Logger log = LoggerFactory.getLogger(DatabusMapredTest.class);

    	static long reducecounter=0;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
        	reducecounter++;
        	if (reducecounter%1000 == 1)
        		log.info("called reduce "+reducecounter+" times.");
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            context.write(key, new IntWritable(sum));
        }
    }