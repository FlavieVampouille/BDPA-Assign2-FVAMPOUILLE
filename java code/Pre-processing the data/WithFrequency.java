package PreProcessing;


import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;


public class WithFrequency extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(),
				new WithFrequency(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "WithFrequency");

		job.setJarByClass(WithFrequency.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		job.waitForCompletion(true);
		
		return 0;
	}

	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		// HashMap of WordCount
		public static HashMap<String,Integer> WordCount = new HashMap<String, Integer>();
		public static void importWordCount () throws IOException, InterruptedException {
			BufferedReader BR = new BufferedReader(new FileReader(
					new File("/home/cloudera/workspace/Assignment2/WordCount/WordCount.txt")));
			String line1;
			while((line1 = BR.readLine()) != null){
				String[] arr = line1.split(" ");
				String word = new String(arr[0]);
				Integer count = new Integer(Integer.valueOf(arr[1]));
				WordCount.put(word,count);
			}
		}
		static {
			try { 
				importWordCount();
			}
			catch (IOException|InterruptedException e) { throw new ExceptionInInitializerError(e); }
		}
		
		Integer line_count = 0;
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			// pre-process the input
			for (String line : value.toString().toLowerCase().replaceAll("[^a-zA-Z0-9\\s]", "").split("[\\r\\n]+")) {
				String[] words_in_line = line.split(" ");
				// Map of words in line with their global frequency, non sorted
				// remove stop words and words not in WordCount
				HashSet<String> map = new HashSet<String>();
				for (int i = 0; i < words_in_line.length; i++) {
					if (WordCount.containsKey(words_in_line[i])) {
						if (WordCount.get(words_in_line[i]) < 4000 ) {
							map.add(words_in_line[i]);
						}
					}
				}
				
				// create the comparator for global frequency
				Comparator<String> compare_frequency = new Comparator<String>() {
					public int compare (String word1, String word2) {
						return WordCount.get(word1) - WordCount.get(word2);
					}
				};
				// create new output
			    if (!map.isEmpty()) {
			    	line_count++;
			    	List<String> list = new ArrayList<String>(map);
			    	Collections.sort(list, compare_frequency);
			    	StringBuilder SB = new StringBuilder();
			    	String separator = " ";
			    	for (String word : list) {
			    		SB.append(word);
			    		SB.append(separator);
			    	}
			    	// write line ID and line content's
			    	context.write(new Text(String.valueOf(line_count)),new Text(SB.toString()));
			    }
			}
		}
	}
}
