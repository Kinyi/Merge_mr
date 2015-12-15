package com.asiainfo.ctc.eda.Merge_mr;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

public class MR_merge {

//	static final String INPUT_PATH = "hdfs://BJTEL/apps/hive/warehouse/edaa.db/ddr_ini/yyyymmdd=20151030/ds=3g/";
//	static final String OUT_PATH = "hdfs://tmp/result/3g";
	static final String INPUT_PATH = "hdfs://BJTEL/tmp/result/data";
	static final String OUT_PATH =   "hdfs://BJTEL/tmp/result/kinyi/";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		final Job job = Job.getInstance(conf, MR_merge.class.getSimpleName());
		final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), new Configuration());
		final Path path = new Path(OUT_PATH);
		if (fileSystem.exists(path)) {
			fileSystem.delete(path, true);
		}
		job.setJarByClass(MR_merge.class);
		
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
//		job.setNumReduceTasks(1);
		
//		job.setNumReduceTasks(0);
		
		// 1.4排序、分组
		// 1.5归约（可选）
		// 2.2指定自定义的reducer类
		job.setReducerClass(MyReducer.class);
		
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

			//TODO 输入格式不一定是这些,根据文件格式而定
			//流量或者其他必要字段
//			String[] split = v1.toString().split("\\u005");
			String[] split = v1.toString().split("\\t");
			String billing_nbr = split[0];
			String business_key = split[1];
			String start_time = split[2];
			String end_time = split[3];
			String send_bytes = split[4];
			String recv_bytes = split[5];
			String raw_flux = split[6];
			String roam_type = split[7];
			String ip_address = split[8];

			//map输出的key
			String key = billing_nbr + ";" + ip_address;
			//map输出的value--整个字段,后面两次遍历时用于判断该记录是否本身记录
			String value = billing_nbr + ";" + business_key + ";" + start_time + ";" + end_time + ";" + send_bytes + ";"
					+ recv_bytes + ";" + raw_flux + ";" + roam_type + ";" + ip_address;

			context.write(new Text(key), new Text(value));
		}
	}

	/**
	 * KEYIN 即k2 表示行中出现的单词 VALUEIN 即v2 表示行中出现的单词的次数 KEYOUT 即k3 表示文本中出现的不同单词
	 * VALUEOUT 即v3 表示文本中出现的不同单词的总次数
	 * 
	 */
	static class MyReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		//reduce中的迭代器不能迭代两次,第二次为空值
		/**	如果想要在reduce端进行两次迭代,需要新建两个list用于接收迭代器里面的值,而且不能直接赋引用(Text value = text;),
		 *	需要将其中的值取出另存或者重新clone一个对象(String value = text.toString();)
		 *	我理解:toString()重新构建了一个对象,具体原理我也不清楚
		 */
		List<String> out_list = new ArrayList<String>();
        List<String> in_list = new ArrayList<String>();

		@Override
		protected void reduce(Text k2, Iterable<Text> v2s, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			//每一组reduce都需要把之前的list值清除
			out_list.clear();
			in_list.clear();
			
			for (Text text : v2s) {
				String value = text.toString();
				out_list.add(value);
				in_list.add(value);
			}
			
			//对存储value值的两个list进行排序(按起始时间),合并逻辑用到
			Collections.sort(out_list, new Comparator<String>() {

				public int compare(String o1, String o2) {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					
					Date date1 = null;
					Date date2 = null;
					try {
						date1 = sdf.parse(o1.substring(15,34));
						date2 = sdf.parse(o2.substring(15,34));
					} catch (ParseException e) {
						e.printStackTrace();
					}
					return date1.compareTo(date2);
				}
			});
			
			Collections.sort(in_list, new Comparator<String>() {

				public int compare(String o1, String o2) {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					
					Date date1 = null;
					Date date2 = null;
					try {
						date1 = sdf.parse(o1.substring(15,34));
						date2 = sdf.parse(o2.substring(15,34));
					} catch (ParseException e) {
						e.printStackTrace();
					}
					return date1.compareTo(date2);
				}
			});
			
			/*for (String str : out_list) {
				context.write(new Text(str), NullWritable.get());
			}
			context.write(new Text("------------"), NullWritable.get());*/
			
//			Iterator<Text> out_iterator = v2s.iterator();
			
			// 合并后的记录
			Set<String> merged_set = new HashSet<String>();
			// 合并过的记录
			Set<String> repeat_set = new HashSet<String>();
			
			//日期格式化相关
//			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
//			int i = 0;
			
//			for (Text outside : v2s) {
//			while (out_iterator.hasNext()) {
			
			/**
			 * 比较逻辑：
			 * 相当于两次for循环,原则上让每一条记录都有机会去匹配除自己以外的其他记录
			 * 外层循环的记录去匹配内层循环的所有记录,匹配过的内层记录存储于repeat_set中,用于外层循环判断该记录是否应该开始外层循环(被合并之后的记录不应该再去匹配其他记录,否则重复)
			 * 注意点:list应该有序
			 */
			for (String whole_out : out_list) {
				
//				String whole_out = outside.toString();
//				String whole_out = out_iterator.next().toString();
				
//				i++;
//				context.write(new Text(whole_out+":"+i), NullWritable.get());
				
				//判断是否已经被合并过
				if (!repeat_set.contains(whole_out)) {
					
					String[] split_out = whole_out.split(";");
					long start_out = timestamp_trans(split_out[2]);
					long end_out = timestamp_trans(split_out[3]);
					long flux_out = Long.parseLong(split_out[6]);
					
					/*long start_out = 0L;
					long end_out = 0L;
					long flux_out = 0L;
					try {
						start_out = sdf.parse(split_out[2]).getTime();
						end_out = sdf.parse(split_out[3]).getTime();
						flux_out = Long.parseLong(split_out[6]);
					} catch (ParseException e) {
						e.printStackTrace();
					}*/
					
					// 合并后的流量
					long flux_total = flux_out;
					// 合并后的起始时间
					long start_merged = start_out;
					// 合并后的结束时间
					long end_merged = end_out;
					
//					Iterator<Text> in_iterator = v2s.iterator();
					
//					for (Text inside : in_iterable) {
//					while (in_iterator.hasNext()) {
					
					for (String whole_in : in_list) {
						
//						String whole_in = inside.toString();
//						String whole_in = in_iterator.next().toString();
						
						String[] split_in = whole_in.split(";");
						
						long start_in = timestamp_trans(split_in[2]);
						long end_in = timestamp_trans(split_in[3]);
						long flux_in = Long.parseLong(split_in[6]);
						
						/*long start_in = 0L;
						long end_in = 0L;
						long flux_in = 0L;
						try {
							start_in = sdf.parse(split_in[2]).getTime();
							end_in = sdf.parse(split_in[3]).getTime();
							flux_in = Long.parseLong(split_in[6]);
						} catch (ParseException e) {
							e.printStackTrace();
						}*/

						// 不是自己本身的那条记录
						if (!whole_in.equals(whole_out)) {
							// 两条记录相互比较
							if (start_merged >= start_in && start_merged <= end_in
									|| start_in >= start_merged && start_in <= end_merged) {
								flux_total = flux_total + flux_in;
								start_merged = Math.min(start_in, start_merged);
								end_merged = Math.max(end_in, end_merged);
								// 把合并过的记录保存到repeat_set
								repeat_set.add(whole_in);
							}
						}
					}
					// 把比较过一轮的外层记录保存到merged_set
					merged_set.add(split_out[0] + "\t" + format(start_merged) + "\t" + format(end_merged) + "\t" + flux_total + "\t" + split_out[8]);
//					merged_set.add(split_out[0] + "\t" + sdf.format(new Date(start_merged)) + "\t" + sdf.format(new Date(end_merged)) + "\t" + flux_total + "\t" + split_out[8]);
//					context.write(new Text(split_out[0] + "\t" + sdf.format(new Date(start_merged)) + "\t" + sdf.format(new Date(end_merged)) + "\t" + flux_total + "\t" + split_out[8]), NullWritable.get());
				}else{
					continue;
				}
			}
			
//			context.write(new Text(i+""), NullWritable.get());
			
			for (String record : merged_set) {
				context.write(new Text(record), NullWritable.get());
			}
			
//			context.write(new Text("--------------------------"), NullWritable.get());
			/*
			for (String string : repeat_set) {
				context.write(new Text(string), NullWritable.get());
			}*/
		}

		//时间转换方法
		static long timestamp_trans(String input) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date parse = null;
			try {
				parse = sdf.parse(input);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return parse.getTime();
		}

		static String format(long timestamp) {
			Date date = new Date(timestamp);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String format = sdf.format(date);
			return format;
		}
	}
}