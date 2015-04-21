import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class hw1_phase2{
	public static class Map_2 extends Mapper<Object, Text, Text, Text> {
		private Text key2 = new Text();
		private Text value2 = new Text();
		
		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		    String line=value.toString();
		    String element[] = null;
			element = line.split(",");
			key2.set(element[0]);
			
			String valsplit[] =element[2].split("\t");
			value2.set(valsplit[1]);
			
				
			context.write(key2,value2);
			//System.out.println("hihihi");
			//System.out.println(key2+":"+value2);
					
				     }
		}
			
	
	public static class Reduce_2 extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
	//		System.out.println("At the reducer: "+key);			
			double avg=0;
			double sum=0;
			double volatility=0;
			List<Double> xi = new ArrayList<Double>();
			
			for(Text value:values) {
		//		System.out.println("At the reducer key:"+key+"Value:"+value);
		    xi.add(Double.parseDouble(value.toString()));}
			for(int i=0;i<xi.size();i++) 
				{avg=(avg+xi.get(i));}
			
			avg=avg/(double)xi.size();
			
			for(int j=0;j<xi.size();j++) {
				double diff=xi.get(j)-avg;
				double square=Math.pow(diff, 2);
				sum=sum+square;}
			
			volatility=Math.sqrt(sum/(xi.size()-1));
		//	System.out.println(key.toString()+" "+volatility);		
			context.write(key,new Text(String.valueOf(volatility)));
			
		}
	}


		
		
		
	
}