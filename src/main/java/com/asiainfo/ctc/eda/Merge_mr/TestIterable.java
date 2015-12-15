package com.asiainfo.ctc.eda.Merge_mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TestIterable {
	 
    public static class M1 extends Mapper<Object, Text, Text, Text> {
        private Text oKey = new Text();
        private Text oVal = new Text();
        String[] lineArr;
 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            lineArr = value.toString().split(" ");
            oKey.set(lineArr[0]);
            oVal.set(lineArr[1]);
            context.write(oKey, oVal);
        }
    }
 
    public static class R1 extends Reducer<Text, Text, Text, Text> {
        List<String> valList = new ArrayList<String>();
        List<String> valList2 = new ArrayList<String>();
        List<Text> textList = new ArrayList<Text>();
        String strAdd;
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            valList.clear();
            valList2.clear();
//            textList.clear();
            strAdd = "";
            for (Text val : values) {
                valList.add(val.toString());
                valList2.add(val.toString());
//                textList.add(val);
            }
            
            int i = 0;
            
            for (String out : valList) {
            	i++;
				for (String in : valList2) {
					context.write(new Text(out+":"+i), new Text(in));
				}
			}
            
            
            
            
            
             
            /*// 坑之 1 ：为神马输出的全是最后一个值？why？
            for(Text text : textList){
                strAdd += text.toString() + ", ";
            }
//            System.out.println(key.toString() + "\t" + strAdd);
//            System.out.println(".......................");
            
            context.write(new Text(key.toString() + "\t" + strAdd), new Text());
             
            // 我这样干呢？对了吗？
            strAdd = "";
            for(String val : valList){
                strAdd += val + ", ";
            }
//            System.out.println(key.toString() + "\t" + strAdd);
//            System.out.println("----------------------");
            
            context.write(new Text(key.toString() + "\t" + strAdd), new Text());
            
            strAdd = "";
            for(String val : valList2){
                strAdd += val + ", ";
            }
            context.write(new Text(key.toString() + "\t" + strAdd), new Text());
            
            // 坑之 2 ：第二次遍历的时候为什么得到的都是空？why？
            valList.clear();
            strAdd = "";
            for (Text val : values) {
                valList.add(val.toString());
            }
            for(String val : valList){
                strAdd += val + ", ";
            }
//            System.out.println(key.toString() + "\t" + strAdd);
//            System.out.println(">>>>>>>>>>>>>>>>>>>>>>");
            
            context.write(new Text(key.toString() + "\t" + strAdd), new Text());*/
        }
    }
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        conf.set("mapred.job.queue.name", "regular");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        System.out.println("------------------------");
        Job job = Job.getInstance(conf, TestIterable.class.getSimpleName());
        job.setJarByClass(TestIterable.class);
        job.setMapperClass(M1.class);
        job.setReducerClass(R1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 输入输出路径
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileSystem.get(conf).delete(new Path(otherArgs[1]), true);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}