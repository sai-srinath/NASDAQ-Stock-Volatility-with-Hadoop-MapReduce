import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

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
public class hw1_phase1{
	public static class Map_1 extends Mapper<Object, Text, Text, Text> {
		private Text key1 = new Text();
		private Text value1 = new Text();
		
		
		String current_month=null;
		String prev_line=null;
		
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString(); 
		if(line!=null){
		if(line.charAt(0)!='D' ){
		try{
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String filename = fileSplit.getPath().getName();
		
		String element[] = null;
		element = line.split(",");
		String date[]=element[0].split("-");
		
		//if(date[0]!=null && date[1]!=null &&date[2]!=null && element[6]!=null){
	
	//	String k =filename+","+date[1]+","+date[0];
		
	//	String v =date[2]+","+element[6]; 
		
		key1.set(filename+","+date[1]+","+date[0]);
		value1.set(date[2]+","+element[6]);
		context.write(key1,value1); 
		//}

		
		//System.out.println("Stage 1 map: Key "+k +"Value " +v);
		}      
		catch(ArrayIndexOutOfBoundsException e)
		{e.printStackTrace();
			
		}

		
		}}
		
			
		}
	}
		
	

	
	public static class Reduce_1 extends Reducer<Text, Text, Text, Text> {
		private Text value_x = new Text();
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			 HashMap<Integer, Double> map = new HashMap<Integer, Double>();
			
			for(Text value:values){
				String string = value.toString();
				String slist[]=string.split(",");
				map.put(Integer.parseInt(slist[0]),Double.parseDouble(slist[1]));
			}
			int max=0;
			int min=50;
			double x=0;
			for(int i:map.keySet()){
				if(max<i)
			     max=i; }
			  
			for(int i:map.keySet()){
				if(min>i)
			     min=i; }
			x=(map.get(max)-map.get(min))/map.get(min);
			
			value_x.set(String.valueOf(x));
			context.write(key,value_x);
			
			
		}
	}
}
		
		
		
		
	







