import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgAge {
    public static class AgeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static Text ageKey = new Text("age");
        private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private final static Date refDate = new Date(1735689600000L); // 2025-01-01

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length < 2) return;
            try {
                Date dob = format.parse(parts[1]);
                int age = (int) ((refDate.getTime() - dob.getTime()) / (1000L * 60 * 60 * 24 * 365));
                context.write(ageKey, new IntWritable(age));
            } catch (Exception ignored) {}
        }
    }

    public static class AgeReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0, count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            if (count > 0) {
                context.write(new Text("Average Age"), new DoubleWritable((double) sum / count));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Age");
        job.setJarByClass(AvgAge.class);
        job.setMapperClass(AgeMapper.class);
        job.setReducerClass(AgeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
