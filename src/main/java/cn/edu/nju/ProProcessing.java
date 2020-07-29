package cn.edu.nju;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.StringTokenizer;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.ansj.library.UserDefineLibrary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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

public class ProProcessing {
    public static class ProProcessingMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String nameList = context.getConfiguration().get("namelist");
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            FSDataInputStream inputstream = fileSystem.open(new Path(nameList));
            BufferedReader br = new BufferedReader(new InputStreamReader(inputstream));
            String nameline;
            while ((nameline = br.readLine()) != null) {
                UserDefineLibrary.insertWord((new StringTokenizer(nameline)).nextToken(), "names", 1000);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Result result = DicAnalysis.parse(line);
            List<Term> terms = result.getTerms();
            StringBuilder sb = new StringBuilder();
            if (terms.size() > 0) {
                for (int i = 0; i < terms.size(); i++) {
                    String word = terms.get(i).getName(); // 拿到词
                    String natureStr = terms.get(i).getNatureStr(); // 拿到词性
                    if (natureStr.equals("names")) {
                        sb.append(word + " ");
                    }
                }
            }
            String res = sb.length() > 0 ? sb.toString().substring(0, sb.length() - 1) : "";
            context.write(new Text(res), NullWritable.get());
        }
    }

    public static class ProProcessingReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args)throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length!=4){
            System.err.println("Usage: ProProcessing <in> <namelist> <out>");
            System.exit(2);
        }
        Job job =Job.getInstance(conf,"ProProcessing");
        job.setJarByClass(ProProcessing.class);
        job.setMapperClass(ProProcessingMapper.class);
        job.setReducerClass(ProProcessingReducer.class);

        //job.setPartitionerClass();
        //job.setGroupingComparatorClass();

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
        job.getConfiguration().set("namelist", otherArgs[2]);

        Path out = new Path(otherArgs[3]);
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(out)){
            fileSystem.delete(out,true);
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}