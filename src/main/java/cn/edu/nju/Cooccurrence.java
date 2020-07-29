package cn.edu.nju;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Cooccurrence {
    public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Set<String> set = new HashSet<>();
            String line = value.toString();
            String[] names = line.split(" ");
            set.addAll(Arrays.asList(names));
            for (String name : set) {
                for (String other_name : set) {
                    if (name.equals(other_name)) {
                        continue;
                    } else {
                        context.write(new Text(name + "," + other_name), new IntWritable(1));
                    }
                }
            }
        }
    }

    public static class CooccurrenceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable i : values) {
                count += i.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args)throws Exception{
        Configuration conf=new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length!=3){
            System.err.println("Usage: Cooccurrence <in> <out>");
            System.exit(2);
        }
        Job job =Job.getInstance(conf,"Cooccurrence");
        job.setJarByClass(Cooccurrence.class);
        job.setMapperClass(CooccurrenceMapper.class);
        job.setReducerClass(CooccurrenceReducer.class);

        //job.setPartitionerClass();
        //job.setGroupingComparatorClass();

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        Path out = new Path(otherArgs[2]);
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(out)){
            fileSystem.delete(out,true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}