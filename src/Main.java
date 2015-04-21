import java.io.IOException;
import java.util.Date;







import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Main{
	
	public static void main(String[] args) throws Exception {	
	
		long start = new Date().getTime();		
		Configuration conf = new Configuration();
		
		 Job job = Job.getInstance();
	     job.setJarByClass(hw1_phase1.class);
	     Job job2 = Job.getInstance();
	     job2.setJarByClass(hw1_phase2.class);
	     Job job3 = Job.getInstance();
	     job3.setJarByClass(hw1_phase3.class);
	     
	     System.out.println("\n:::::::::Stock_Volatility_Hadoop-> Start:::::::::\n");
	     
	        job.setJarByClass(hw1_phase1.class);
			job.setMapperClass(hw1_phase1.Map_1.class);
			job.setReducerClass(hw1_phase1.Reduce_1.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			int NOfReducer1 = 1; 	
			job.setNumReduceTasks(NOfReducer1);
			
			job2.setJarByClass(hw1_phase2.class);
			job2.setMapperClass(hw1_phase2.Map_2.class);
			job2.setReducerClass(hw1_phase2.Reduce_2.class);
			
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			int NOfReducer2 = 1; 
			job2.setNumReduceTasks(NOfReducer2);
			
			job3.setJarByClass(hw1_phase3.class);
			job3.setMapperClass(hw1_phase3.Map_3.class);
			job3.setReducerClass(hw1_phase3.Reduce_3.class);
			
			job3.setMapOutputKeyClass(Text.class);
			job3.setMapOutputValueClass(Text.class);
			int NOfReducer3 =1;;	
			job3.setNumReduceTasks(NOfReducer3);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));

			FileOutputFormat.setOutputPath(job,new Path("Stage1"+args[1]) );
			FileInputFormat.addInputPath(job2, new Path("Stage1"+args[1]));
			FileOutputFormat.setOutputPath(job2, new Path("Stage2"+args[1]));
			FileInputFormat.addInputPath(job3, new Path("Stage2"+args[1]));
			FileOutputFormat.setOutputPath(job3, new Path("Output_Final"+args[1]));
			
			job.waitForCompletion(true);
			job2.waitForCompletion(true);
			boolean status = job3.waitForCompletion(true);
			if (status == true) {
				long end = new Date().getTime();
				System.out.println("\nJob took " + (end-start)/1000 + "seconds\n");
			}
			System.out.println("\n:::::::::Stock_Volatility_Hadoop-> End:::::::::\n");
			
	
		
	}
}
