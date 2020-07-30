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
import org.w3c.dom.NameList;

public class PageRank {
    public static int maxTime = 10;

    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.length() > 0) {
                int index_t = line.indexOf("\t");
                int index_l = line.indexOf("[");
                int index_r = line.indexOf("]");
                // int index_j = line.indexOf("#");
                // double PR = Double.parseDouble(line.substring(index_t + 1, index_j));
                double PR = Double.parseDouble(line.substring(index_t + 1, index_l));
                String name = line.substring(0, index_t);
                String names = line.substring(index_l + 1, index_r);
                for (String name_value : names.split(";")) {
                    String[] nv = name_value.split(":");
                    double relation = Double.parseDouble(nv[1]);
                    double cal = PR * relation;
                    context.write(new Text(nv[0]), new Text(String.valueOf(cal)));
                }
                context.write(new Text(name), new Text("#" + line.substring(index_l)));
            }
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String nameList = "";
            double count = 0;
            for (Text text : values) {
                String t = text.toString();
                if (t.charAt(0) == '#') {
                    nameList = t.substring(1);
                    //System.out.println("namelist:"+nameList);
                } else {
                    count += Double.parseDouble(t);
                }
            }
            context.write(key, new Text(String.valueOf(count) + nameList));
        }
    }

    public static void main(String[] args) throws Exception {

        for (int i = 0; i < maxTime; i++) {
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

            if (i == 0) {
                FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[2] + i));
            } else {
                FileInputFormat.addInputPath(job, new Path(otherArgs[2] + (i - 1)));
                FileOutputFormat.setOutputPath(job, new Path(otherArgs[2] + i));
            }
            job.getConfiguration().set("times", String.valueOf(i));

            Path out = new Path(otherArgs[2] + i);
            FileSystem fileSystem = FileSystem.get(conf);
            if (fileSystem.exists(out)) {
                fileSystem.delete(out, true);
            }

            System.out.println("iteration " + i);

            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        }
        System.exit(0);
    }
}