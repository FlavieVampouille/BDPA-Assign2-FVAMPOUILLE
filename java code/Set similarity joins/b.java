package Similarity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;


public class b extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new b(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "b");

		job.getConfiguration().set("mapreduce.map.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3500m");
		
		job.setJarByClass(b.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " : ");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]),true);
		}

		job.waitForCompletion(true);
		
		System.out.print("Number of Performed Comparisons = " +job.getCounters().findCounter(Number.PerformedComparison).getValue());
		
		return 0;
	}

	
	public static Float SimilarityThreshold = 0.8f;
	
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text Index = new Text();

		public void map(LongWritable key, Text value, Context context
                 ) throws IOException, InterruptedException {

			String line = value.toString().split(";")[1];
			String[] WordsLine = line.toString().split(" ");

			int KeepWords = (int) (WordsLine.length - Math.ceil(SimilarityThreshold * WordsLine.length) + 1);

			for (int i = 0; i < KeepWords; i++) {
				Index.set(WordsLine[i]);
				context.write(Index, value);
			}
		}
	}
	
	
	public static enum Number {
		PerformedComparison,
	};
	
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private HashSet<String> duplicate = new HashSet<String>();
		
		@Override
		public void reduce(Text keys, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			List<Integer> IDs = new ArrayList<Integer>();
			List<String> Contents = new ArrayList<String>();
			
			for (Text val : values) {
				IDs.add(Integer.valueOf(val.toString().split(";")[0]));
				Contents.add(val.toString().split(";")[1]);
			}
			
			if (IDs.size() > 1) {
				for (int i = 0; i < IDs.size()-1; i++) {
					for (int j = i+1; j <= IDs.size()-1; j++) {
						
						Integer id1 = IDs.get(i);
						Integer id2 = IDs.get(j);
						String id = String.valueOf(id1) + "/" + String.valueOf(id2);
						String id_reverse = String.valueOf(id2) + "/" + String.valueOf(id1);
					
						if (!(duplicate.contains(id))) {
						
							duplicate.add(id);
							duplicate.add(id_reverse);
							
							context.getCounter(Number.PerformedComparison).increment(1);
							
							String content1 = Contents.get(i);
							List<String> list1 = Arrays.asList(content1.toString().split(" "));
							HashSet<String> set1 = new HashSet<String>(list1);
							
							String content2 = Contents.get(j);
							List<String> list2 = Arrays.asList(content2.toString().split(" "));
							HashSet<String> set2 = new HashSet<String>(list2);
						
							HashSet<String> union = new HashSet<String>(set1);
							union.addAll(set2);
						
							HashSet<String> intersection = new HashSet<String>(set1);
							intersection.retainAll(set2);
						
							float sim = (float) intersection.size() / union.size();
						
							if (sim >= SimilarityThreshold) {
								outputKey.set(id);
								outputValue.set(String.valueOf(sim));
								context.write(outputKey, outputValue);
							}
						}
					}	
				}
			}
		}
	}
}
