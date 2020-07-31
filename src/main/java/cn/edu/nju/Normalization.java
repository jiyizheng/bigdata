package cn.edu.nju;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Normalization {
    public static class NormalizationMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] kv = line.split(",");
            context.write(new Text(kv[0]), new Text(kv[1]));
        }
    }

    public static class NormalizationReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double count = 0;
            StringBuilder sb = new StringBuilder();
            List<String> list = new ArrayList<>();
            for (Text str : values) {
                list.add(str.toString());
                String[] nv = str.toString().split("\\s+");
                count += Integer.parseInt(nv[1]);
            }

            for (String text : list) {
                String[] nv = text.split("\\s+");
                double number = Integer.parseInt(nv[1]);
                double scale = number / count;
                sb.append(nv[0] + ":" + String.format("%.4f", scale) + ";");
            }
            // sb.insert(0, key.toString() + "\t" + "0.1#");
            sb.insert(0, key.toString() + "\t"+0.1+"[");
            String res = sb.toString().substring(0, sb.length() - 1)+"]";

            context.write(new Text(res), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: Normalization <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Normalization");
        job.setJarByClass(Normalization.class);
        job.setMapperClass(NormalizationMapper.class);
        job.setReducerClass(NormalizationReducer.class);

        // job.setPartitionerClass();
        // job.setGroupingComparatorClass();

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        Path out = new Path(otherArgs[2]);
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
