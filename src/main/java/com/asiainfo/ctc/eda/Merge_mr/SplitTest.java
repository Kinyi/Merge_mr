package com.asiainfo.ctc.eda.Merge_mr;

import java.io.IOException;
import java.net.URI;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class SplitTest {

	static final String INPUT_PATH = "hdfs://BJTEL/tmp/result/data3";
	static final String OUT_PATH = "hdfs://BJTEL/tmp/result/kinyi/";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		final Job job = Job.getInstance(conf, SplitTest.class.getSimpleName());
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), new Configuration());
		final Path path = new Path(OUT_PATH);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}
		job.setJarByClass(SplitTest.class);

		// 1.1输入目录在哪里
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		// 指定对输入数据进行格式化处理的类
		job.setInputFormatClass(TextInputFormat.class);
		// 1.2指定自定义的mapper类
		job.setMapperClass(MyMapper.class);
		// 指定map输出的<k,v>类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// 1.3分区
		job.setPartitionerClass(HashPartitioner.class);
		// job.setNumReduceTasks(1);

		job.setNumReduceTasks(0);

		// 1.4排序、分组
		// 1.5归约（可选）
		// 2.2指定自定义的reducer类
		// job.setReducerClass(MyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		// 2.3指定输出的路径
		FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));
		// 指定输出的格式化类
		job.setOutputFormatClass(TextOutputFormat.class);
		// 把作业提交给jobTracker运行
		job.waitForCompletion(true);
	}

	static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable k1, Text v1, Context context) throws java.io.IOException, InterruptedException {

			String[] split = v1.toString().split("\\u0005");
			context.write(new Text(split[0]), new Text(split[8]));
		}
	}

	/**
	 * KEYIN 即k2 表示行中出现的单词 VALUEIN 即v2 表示行中出现的单词的次数 KEYOUT 即k3 表示文本中出现的不同单词
	 * VALUEOUT 即v3 表示文本中出现的不同单词的总次数
	 * 
	 */
	static class MyReducer extends Reducer<Text, Text, Text, NullWritable> {

		@Override
		protected void reduce(Text k2, Iterable<Text> v2s, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

		}

	}
}
