package cn.edu.nju;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {
    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int index_dollar = line.indexOf("$");
            if (index_dollar != -1) {
                line = line.substring(index_dollar + 1);
            }
            int index_t = line.indexOf("\t");
            int index_l = line.indexOf("[");
            int index_r = line.indexOf("]");
            // int index_j = line.indexOf("#");
            // double PR = Double.parseDouble(line.substring(index_t + 1, index_j));
            double PR = 0.1;
            String name = line.substring(0, index_t);
            String names = line.substring(index_l+1,index_r);
            for (String name_value : names.split(";")) {
                // System.out.println(name_value);
                String[] nv = name_value.split(":");
                double relation = Double.parseDouble(nv[1]);
                double cal = PR * relation;
                context.write(new Text(nv[0]), new Text(String.valueOf(cal)));
            }
            context.write(new Text(name), new Text("#" + line.substring(index_t + 1)));
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        // int index = 0;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // String nameList = "";
            double count = 0;
            for (Text text : values) {
                String t = text.toString();
                if (t.charAt(0) != '#') {
                    // nameList = t;
                // } else {
                    count += Double.parseDouble(t);
                }
            }
            // index++;
            // context.write(new Text(index + "$" + key.toString()), new Text(String.valueOf(count) + nameList));
            context.write(key, new Text(String.valueOf(count)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: PageRank <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Cooccurrence");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);

        // job.setPartitionerClass();
        // job.setGroupingComparatorClass();

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        Path out = new Path(otherArgs[2]);
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);
        }

        System.out.println("entering mapreduce");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}