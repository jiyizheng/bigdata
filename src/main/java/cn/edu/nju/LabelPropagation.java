package cn.edu.nju;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.Map.Entry;

public class LabelPropagation {
    public static int maxTime = 3;

    public static class LabelPropagationMapper extends Mapper<Object, Text, Text, Text> {
        HashMap<String, Integer> temp_label1 = new HashMap<String, Integer>();
        HashMap<String, Integer> temp_label2 = new HashMap<String, Integer>();
        FileSystem hdfs;
        String rawTag;

        // 用于初始化键值对列表
        public void setup(Context context) throws IOException {
            rawTag = context.getConfiguration().get("rawtag");
            hdfs = FileSystem.get(context.getConfiguration());
            Scanner tagFile = new Scanner(hdfs.open(new Path(rawTag)), "UTF-8");
            while (tagFile.hasNextLine()) {
                StringTokenizer st0 = new StringTokenizer(tagFile.nextLine());
                String word = st0.nextToken();
                String tag = st0.nextToken();
                temp_label1.put(word, Integer.parseInt(tag));
            }
            tagFile.close();
        }

        // 用于map集群计算
        public void map(Object key, Text value, Context context) {
            String line = value.toString();
            int index_t = line.indexOf("\t");
            int index_l = line.indexOf("[");
            int index_r = line.indexOf("]");

            String mainName = line.substring(0, index_t);
            String names = line.substring(index_l + 1, index_r);
            StringTokenizer st1 = new StringTokenizer(names, ";");
            if (!temp_label1.containsKey(mainName)) {
                System.err.println(("error: " + mainName + " not found at " + context.getConfiguration().get("times")));
                // System.exit(2);
                return;
            }
            double[] my_list = new double[15];
            for (int i = 0; i < 15; i++)
                my_list[i] = 0;
            while (st1.hasMoreTokens()) {
                StringTokenizer st2 = new StringTokenizer(st1.nextToken(), ":");
                String conName = st2.nextToken();
                double conValue = Double.parseDouble(st2.nextToken());
                if (!temp_label1.containsKey(conName))
                    continue;
                int temp_value = temp_label1.get(conName);
                my_list[temp_value] += conValue;
            }
            int temp_max = 1;
            for (int i = 2; i < 15; i++) {
                if (my_list[i] > my_list[temp_max])
                    temp_max = i;
            }
            if (my_list[temp_max] > 0)
                temp_label2.put(mainName, temp_max);
            else
                temp_label2.put(mainName, 0);
        }

        public void cleanup(Context context) throws IOException {
            hdfs.delete(new Path(rawTag), true);
            FSDataOutputStream out_put = hdfs.create(new Path(rawTag));
            PrintWriter pr1 = new PrintWriter(out_put);
            Set<Entry<String, Integer>> set = temp_label2.entrySet();
            Iterator<Entry<String, Integer>> iterator = set.iterator();
            while (iterator.hasNext()) {
                Entry<String, Integer> entry = iterator.next();
                pr1.println(new String(entry.getKey() + " " + entry.getValue()));
            }
            pr1.close();
            out_put.close();
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        for (int i = 0; i < maxTime; i++) {
            Configuration conf = new Configuration();
            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (otherArgs.length != 4) {
                System.err.println(("Usage: LPA <in> <rawtag> <out>"));
                System.exit(2);
            }
            Job job = new Job(conf, "LPA");

            job.setJarByClass(LabelPropagation.class);
            job.setMapperClass(LabelPropagationMapper.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[3] + "/" + i));
            job.getConfiguration().set("rawtag", otherArgs[2]);
            job.getConfiguration().set("times", String.valueOf(i));

            Path out = new Path(otherArgs[3]);
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