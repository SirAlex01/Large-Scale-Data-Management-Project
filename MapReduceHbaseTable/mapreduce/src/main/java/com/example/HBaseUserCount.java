package com.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
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
        private Text zipCode = new Text();

        @Override
        public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String zip = Bytes.toString(value.getValue(Bytes.toBytes("address"), Bytes.toBytes("zip")));
            if (zip != null) {
                zipCode.set(zip);
                context.write(zipCode, one);
            }
        }
    }

    public static class UserReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
        private Connection connection;
        private Table table;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration config = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(config);
            
            table = connection.getTable(TableName.valueOf("zip_counts"));
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // Write to HBase
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("result"), Bytes.toBytes("count"), Bytes.toBytes(sum));
            table.put(put);

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "gab-IdeaPad-3-15ADA05,alex-VirtualBox,biar");

        Job job = Job.getInstance(config, "HBase User Count");
        job.setJarByClass(HBaseUserCount.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("address")); // Adjust if needed to scan specific columns

        TableMapReduceUtil.initTableMapperJob(
                "users",        // Input table
                scan,           // Scan instance to control CF and attribute selection
                UserMapper.class,  // Mapper class
                Text.class,     // Mapper output key
                IntWritable.class, // Mapper output value
                job);

        TableMapReduceUtil.initTableReducerJob(
                "zip_counts",   // Output table
                UserReducer.class, // Reducer class
                job);

        job.setReducerClass(UserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
    	TableName tableName = TableName.valueOf("zip_counts");
            
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("result")).build())
                .build();
        connection.getAdmin().createTable(tableDescriptor);
        connection.close();
        
        int exitCode = ToolRunner.run(new HBaseUserCount(), args);
        System.exit(exitCode);
    }
}

