package com.tianchi.hyr.recommend;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * 按照推荐得分降序排序，每个用户列出10个推荐物品
 * According to the recommended score descending order, each user lists 10 recommended items
 * 
 * @author root
 */
public class Step6 {
	private final static Text K = new Text();
	private final static Text V = new Text();

	public static boolean run(Configuration config, Map<String, String> paths) {
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			job.setJobName("step6");
			job.setJarByClass(StartRun.class);
			job.setMapperClass(Step6_Mapper.class);
			job.setReducerClass(Step6_Reducer.class);
			job.setSortComparatorClass(NumSort.class);
			job.setGroupingComparatorClass(UserGroup.class); // 根据用户进行分组	Grouping according to user
			job.setMapOutputKeyClass(PairWritable.class);
			job.setMapOutputValueClass(Text.class);

			FileInputFormat
					.addInputPath(job, new Path(paths.get("Step6Input")));
			Path outpath = new Path(paths.get("Step6Output"));
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

	static class Step6_Mapper extends
			Mapper<LongWritable, Text, PairWritable, Text> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = Pattern.compile("[\t,]").split(value.toString());
			String u = tokens[0];
			String item = tokens[1];
			String num = tokens[2];
			PairWritable k = new PairWritable(); // 自定义的排序对象 二次排序		Custom sorting object two sorting
			k.setUid(u);
			k.setNum(Double.parseDouble(num));
			V.set(item + ":" + num);
			context.write(k, V);
		}
	}

	static class Step6_Reducer extends Reducer<PairWritable, Text, Text, Text> {
		@Override
		protected void reduce(PairWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int i = 0;
			StringBuffer sb = new StringBuffer();
			for (Text v : values) { // 取出前10条进行推荐	Take out the first 10 to recommend
				if (i == 10)
					break;
				sb.append(v.toString() + ",");
				i++;
			}
			K.set(key.getUid());
			V.set(sb.toString());
			context.write(K, V);
		}

	}

	static class PairWritable implements WritableComparable<PairWritable> {

		private String uid;
		private double num;

		public void write(DataOutput out) throws IOException {
			out.writeUTF(uid);
			out.writeDouble(num);
		}

		public void readFields(DataInput in) throws IOException {
			this.uid = in.readUTF();
			this.num = in.readDouble();
		}

		public int compareTo(PairWritable o) {
			int r = this.uid.compareTo(o.getUid());
			if (r == 0) {
				return Double.compare(this.num, o.getNum());
			}
			return r;
		}

		public String getUid() {
			return uid;
		}

		public void setUid(String uid) {
			this.uid = uid;
		}

		public double getNum() {
			return num;
		}

		public void setNum(double num) {
			this.num = num;
		}

	}

	/**
	 * 得分比较降序	The scores are in descending order
	 * @author huangyueran
	 *
	 */
	static class NumSort extends WritableComparator {
		public NumSort() {
			super(PairWritable.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			PairWritable o1 = (PairWritable) a;
			PairWritable o2 = (PairWritable) b;

			int r = o1.getUid().compareTo(o2.getUid());
			if (r == 0) {
				return -Double.compare(o1.getNum(), o2.getNum());
			}
			return r;
		}
	}

	/**
	 * 用户比较	User comparison
	 * @author huangyueran
	 *
	 */
	static class UserGroup extends WritableComparator {
		public UserGroup() {
			super(PairWritable.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			PairWritable o1 = (PairWritable) a;
			PairWritable o2 = (PairWritable) b;
			return o1.getUid().compareTo(o2.getUid());
		}
	}
}