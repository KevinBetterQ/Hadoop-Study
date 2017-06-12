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
 * @author daT dev.tao@gmail.com
 *
 */
public class Sort{
	
	public static class SortMapper extends Mapper<Object, Text,  FloatWritable,Text>{
		
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
				Text outName = new Text(name);
				FloatWritable outScore  = new FloatWritable(score);
				context.write(outScore,outName);
			}
		}
		
	}
	
	public static class SortReducer extends Reducer<FloatWritable,Text, FloatWritable, Text>{
		@Override
		protected void reduce(FloatWritable key, Iterable<Text> value,Context context)
				throws IOException, InterruptedException {
			
			for(Text i:value){
				context.write(key,i);
			}
		}
		
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		System.out.println("Begin");
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length<2){
			System.out.println("please input at least 2 arguments");
			System.exit(2);
		}
		
		Job job = new Job(conf,"sort");
		//job.setJarByClass(AverageScore.class);
		job.setMapperClass(SortMapper.class);
		job.setCombinerClass(SortReducer.class);
		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(FloatWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		System.out.println("End");
	}
	
}
