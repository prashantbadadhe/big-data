
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MissingPokerCards {
	public static class myMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// value = (HEART,2)
			// value = (Diamond,3)
			String line = value.toString();
			String[] lineSplit = line.split(",");
			context.write(new Text(lineSplit[0]), new IntWritable(Integer.parseInt(lineSplit[1])));
		}
	}

	 public static class myReducer extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {

	        public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
		
				throws IOException, InterruptedException {
			// Input to Reducer = (HEART,(1,2,3,4,7,8,9,11,12));
			// Input to Reducer = (DIAMOND,(1,2,3,4,7,9,11,12));
			ArrayList<Integer> nums = new ArrayList<Integer>();
			int sum = 0;
			int tempVal = 0;
			for (IntWritable val : values) {
				sum += val.get();
				tempVal = val.get();
				nums.add(tempVal);
			}

			if (sum < 91) {
				for (int i = 1; i <= 13; i++) {
					if (!nums.contains(i)) {
						Put put = new Put(Bytes.toBytes(nums.get(i)));
						put.add(Bytes.toBytes(i), Bytes.toBytes(nums.get(i)), Bytes.toBytes(nums.get(i)));
						 context.write(key, put);
					}
				}
			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf, "MissingPockerCards");
		job.setJarByClass(MissingPokerCards.class);
		job.setMapperClass(myMapper.class);
		job.setReducerClass(myReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		TableMapReduceUtil.initTableReducerJob("MissingPockerCards", myReducer.class, job);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
