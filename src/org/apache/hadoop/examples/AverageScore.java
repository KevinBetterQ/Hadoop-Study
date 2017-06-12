package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 计算学生的平均成绩
 * 学生成绩以每科一个文件输入
 * 文件内容为：姓名 成绩
 * @author KevinQi
 *
 */
public class AverageScore {
	
	public static class AverageMapper extends Mapper<Object, Text, Text, FloatWritable>{
		
		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokens = new StringTokenizer(line,"\n");
			while(tokens.hasMoreTokens()){
				String tmp = tokens.nextToken();
				StringTokenizer sz = new StringTokenizer(tmp);
				String name = sz.nextToken();
				float score = Float.valueOf(sz.nextToken());
				Text outName = new Text("average");
				FloatWritable outScore  = new FloatWritable(score);
				context.write(outName, outScore);
			}
		}
		
	}
	
	public static class AverageReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{
		@Override
		protected void reduce(Text key, Iterable<FloatWritable> value,Context context)
				throws IOException, InterruptedException {
			float sum = 0;
			int count = 0;
			for(FloatWritable f:value){
				sum += f.get();
				count ++;
			}
			FloatWritable averageScore = new FloatWritable(sum/count);
			context.write(key, averageScore);
		}
		
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length<2){
			System.out.println("please input at least 2 arguments");
			System.exit(2);
		}
		
		Job job = new Job(conf,"Average Score");
		job.setJarByClass(AverageScore.class);
		job.setMapperClass(AverageMapper.class);
		job.setCombinerClass(AverageReducer.class);
		job.setReducerClass(AverageReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
	
}
