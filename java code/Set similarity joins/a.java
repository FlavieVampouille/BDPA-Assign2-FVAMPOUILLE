package Similarity;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;


public class a extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new a(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "a");

		job.getConfiguration().set("mapreduce.map.memory.mb", "4096");
		job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3500m");
		
		job.setJarByClass(a.class);
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
	
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text IDs = new Text();
		private Text Contents = new Text();
 		
		// HashMap of ID;Content file
		public static HashMap<Integer,String> MapDoc = new HashMap<Integer, String>();
		public static void importInput () throws IOException, InterruptedException {
			BufferedReader BR = new BufferedReader(new FileReader(
					new File("/home/cloudera/workspace/Assignment2/NewInput/PreProcessPart1000.txt")));
			String line;
			while((line = BR.readLine()) != null){
				String[] arr = line.split(";");
				Integer ID = new Integer(Integer.valueOf(arr[0]));
				String Content = new String(arr[1]);
				MapDoc.put(ID,Content);
			}
		}
		static {
			try { 
				importInput();
			}
			catch (IOException|InterruptedException e) { throw new ExceptionInInitializerError(e); }
		}
		
		@Override
		public void map(LongWritable keys, Text values, Context context)
				throws IOException, InterruptedException {
		
			int id1 = Integer.valueOf(values.toString().split(";")[0]);
			
			for (int id2 = id1+1; id2 <= MapDoc.size(); id2++) {
				IDs.set(String.valueOf(id1) + "/" + String.valueOf(id2));
				Contents.set(MapDoc.get(id1) + "/" + MapDoc.get(id2));
				context.write(IDs,Contents);
			}
		}
	}
	
	
	public static enum Number {
		PerformedComparison,
	};
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		private Text SimilarityOutput = new Text();
		
		@Override
		public void reduce(Text keys, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			String Docs = values.iterator().next().toString();
			String[] splitted = Docs.split("/");
			
			context.getCounter(Number.PerformedComparison).increment(1);
			
			List<String> list1 = Arrays.asList(splitted[0].toString().split(" "));
			HashSet<String> set1 = new HashSet<String>(list1);
			
			List<String> list2 = Arrays.asList(splitted[1].toString().split(" "));
			HashSet<String> set2 = new HashSet<String>(list2);
			
			HashSet<String> union = new HashSet<String>(set1);
			union.addAll(set2);
		
			HashSet<String> intersection = new HashSet<String>(set1);
			intersection.retainAll(set2);
		
			Float sim = (new Float(intersection.size())) / (new Float(union.size()));
			
			if (sim >= 0.8) {					
				SimilarityOutput.set(String.valueOf(sim));
				context.write(keys,SimilarityOutput);
			}
		}
	}
}