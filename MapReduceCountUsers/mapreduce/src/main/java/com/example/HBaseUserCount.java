package com.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseUserCount extends Configured implements Tool {

    public static class UserMapper extends TableMapper<Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text rowKey = new Text();

        @Override
        public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            rowKey.set(Bytes.toString(key.get()));
            context.write(rowKey, one);
        }
    }

    public static class UserReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "alex-VirtualBox,biar");

        Job job = Job.getInstance(config, "HBase User Count");
        job.setJarByClass(HBaseUserCount.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("personal")); // Adjust if needed to scan specific columns

        TableMapReduceUtil.initTableMapperJob(
                "users",        // Input table
                scan,           // Scan instance to control CF and attribute selection
                UserMapper.class,  // Mapper class
                Text.class,     // Mapper output key
                IntWritable.class, // Mapper output value
                job);

        job.setReducerClass(UserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(args[0])); // Output path

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new HBaseUserCount(), args);
        System.exit(exitCode);
    }
}

