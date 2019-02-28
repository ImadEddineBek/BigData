import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

public class FrequencySort {
    public static class FrequencySortMap extends Mapper<Object, Text, IntWritable, Text> {
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(line, "\n");
            while (stringTokenizer.hasMoreTokens()) {
                int frequency = 0;
                String curent = stringTokenizer.nextToken();
                String[] splits = curent.split("\t");
                String word = splits[0];
                int number = Integer.parseInt(splits[1]);
                context.write(new IntWritable(number), new Text(word));
            }
        }
    }


    public static class FreqReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(FrequencySortMap.class);
        job.setMapperClass(FrequencySortMap.class);
        job.setCombinerClass(FreqReducer.class);
        job.setReducerClass(FreqReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(MyKeyComparator.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

class MyKeyComparator extends WritableComparator {
    protected MyKeyComparator() {
        super(IntWritable.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        IntWritable key1 = (IntWritable) w1;
        IntWritable key2 = (IntWritable) w2;
        return -1 * key1.compareTo(key2);
    }
}