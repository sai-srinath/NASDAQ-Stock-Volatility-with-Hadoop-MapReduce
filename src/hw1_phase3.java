import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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

public class hw1_phase3{
	
	public static class Map_3 extends Mapper<Object, Text, Text, Text> {
		Text val3 = new Text();
		Text key3 = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		key3.set(" ");
			
			  
			String s=value.toString();
		  if(s!=null){
			String ele[]=s.split("\t");
		  //  if(ele[1]!=null){		
		    ele[1].trim();
		    		//val3.set(ele[1]);
		    	//	System.out.println("printing map3:"+ele[1]);
		    String ele2[]=ele[0].split("\\.");		
		    //key3.set(ele2[0]);		
		   // System.out.println(ele2[0]);
			val3.set(ele2[0]+"_"+ele[1]);
		   // System.out.println("At mapper 3: key is:"+key3.toString()+"value is"+val3.toString()); 
			context.write(key3,val3);}
			
		}
	}
	
	
	public static class Reduce_3 extends Reducer<Text, Text, Text, Text> {
		static TreeMap<Double,Text> tmap = new TreeMap<Double,Text>();
		 Text v = new Text();
		 static int count=0;
		
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		
			
			
			
			System.out.println("Entering Reduce3");
			
			
			for(Text value:values){
            String s=value.toString();
            String[] kv=s.split("_");
           // System.out.println("the key and value is"+kv[0]+kv[1]);
            if(kv[0]!=null && kv[1]!=null){
            
			tmap.put(Double.parseDouble(kv[1]),new Text(kv[0]));}
           // System.out.println("putting treemap key-value"+ Double.parseDouble(s) +key);
		}
		
		
		for(Double i:tmap.keySet()){
			if(count==0)
				{context.write(new Text("The top 10 socks with the least volatility is:"),new Text(""));}
			//System.out.println(count);
			v.set(String.valueOf(i));
			context.write(tmap.get(i),v);
			
		    count++;
		    if(count>=10)
		    	break; 
			
		  }
		
		count=0;
		
		//for(Double x:tmap.keySet())
			//System.out.println("The key is:"+x +" Value is:" + tmap.get(x));
		
		for(Double i:tmap.descendingKeySet()){
			//System.out.println(count);
			if(count==0)
			{context.write(new Text("The top 10 socks with the most volatility is:"),new Text(""));}
			v.set(String.valueOf(i));
			context.write(tmap.get(i),v);
		  
		    count++;
		    if(count>=10)
		    	break;
		    
		  
			
		  }
		
		
		}
	}	
	
}
	
