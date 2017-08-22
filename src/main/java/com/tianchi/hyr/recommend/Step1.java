package com.tianchi.hyr.recommend;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.xml.internal.bind.CycleRecoverable.Context;

/**
 * 去重	Duplicate removal
 * @author root
 */
public class Step1 {

	public static boolean run(Configuration config, Map<String, String> paths) {
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			
			job.setJobName("step1");

//			config.set("mapred.jar", "D:\\MR\\item.jar");

			job.setJarByClass(Step1.class);
			job.setMapperClass(Step1_Mapper.class);
			job.setReducerClass(Step1_Reducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);

			FileInputFormat.addInputPath(job, new Path(paths.get("Step1Input")));
			
			Path outpath = new Path(paths.get("Step1Output"));
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);

			boolean f = job.waitForCompletion(true);
			return f;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	static class Step1_Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		/**
		 * 用值作为key 去重	Use value as key to reset
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			if (key.get() != 0) {
				context.write(value, NullWritable.get());
			}
		}
	}

	static class Step1_Reducer extends Reducer<Text, IntWritable, Text, NullWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> i, Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
}

