package study;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputSplit; 
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 *mr下求解dem图像坡度，该方法适用于依赖周围八个像元求解中央像元值得情况
 */
public class Gradient {
	public static class Map extends Mapper<Object, Text, Text, Text> {                 
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {           
			String line = value.toString(); 
			if (line.contains("xllcorner") || line.contains("yllcorner") || line == null || line.equals("") || line.contains("NODATA_value")
					|| line.contains("cellsize") || line.contains("ncols") || line.contains("nrows")) {
					return;
			}	
			
			String[] values = line.split(" ");
			int k = values.length;
			int rowindex = Integer.parseInt(values[0]);
			int rowpre = rowindex-1;
			int rownext = rowindex+1;
			
			for (int i=1; i<k; i++) {
				String previous = "";
				String present = "";
				String next = "";
				if (i == 1) {
					previous = "0";
					present = values[i];
					next = values[i+1];
				} else if (i == k-1) {
					previous = values[i-1];
					present = values[i];
					next = "0";
				} else {
					previous = values[i-1];
					present = values[i];
					next = values[i+1];
				}
				
				if (rowindex == 1) {
					context.write(new Text(rowindex+" "+i), new Text(2+" "+previous+" "+present+" "+ next));
					context.write(new Text(rownext+" "+i), new Text(1+" "+previous+" "+present+" "+ next));
					context.write(new Text(rowindex+" "+i), new Text(1+" "+"0"+" "+"0"+" "+ "0"));
				} else if (rowindex == nrows) {
					context.write(new Text(rowindex+" "+i), new Text(2+" "+previous+" "+present+" "+ next));
					context.write(new Text(rowpre+" "+i), new Text(3+" "+previous+" "+present+" "+ next));
					context.write(new Text(rowindex+" "+i), new Text(3+" "+"0"+" "+"0"+" "+ "0"));
				} else {
					context.write(new Text(rowindex+" "+i), new Text(2+" "+previous+" "+present+" "+ next));
					context.write(new Text(rowpre+" "+i), new Text(3+" "+previous+" "+present+" "+ next));
					context.write(new Text(rownext+" "+i), new Text(1+" "+previous+" "+present+" "+ next));
				}
			} 

		}
	}
	
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String result = "";
			double[] a = new double[9];
			Iterator ite = values.iterator();
			while (ite.hasNext()) {
				result = ite.next().toString();
				String[] b = result.split(" ");
				
				//将每个窗口中的九个元素存放到a[0]-a[8]数组中
				if (b[0].contains("1") == true) {
					a[0] = Double.parseDouble(b[1]);
					a[1] = Double.parseDouble(b[2]);
					a[2] = Double.parseDouble(b[3]);
				} else if (b[0].contains("2") == true) {
					a[3] = Double.parseDouble(b[1]);
					a[4] = Double.parseDouble(b[2]);
					a[5] = Double.parseDouble(b[3]);
				} else if (b[0].contains("3") == true) {
					a[6] = Double.parseDouble(b[1]);
					a[7] = Double.parseDouble(b[2]);
					a[8] = Double.parseDouble(b[3]);
				} else {
					return;
				}
				
			}
			
			//若窗口中某一元素的值为0，舍弃该窗口
			for (int i=0; i<9; i++) {
				if (a[i] == 0){
					context.write(key, new Text(0+""));
					return;
				}
			}
			
			//计算坡度
			double fy = ((a[0]-a[6])+2*(a[1]-a[7])+(a[2]-a[8]))/(8*cellsize);
			double fx = ((a[0]-a[2])+2*(a[3]-a[5])+(a[6]-a[8]))/(8*cellsize);
			double beta = Math.sqrt(fx*fx+fy*fy);
			double gradient = Math.atan(beta)/Math.PI*180;

			context.write(key, new Text(""+gradient));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.79.138:9000");
		conf.setInt("ncols", 12001);
		conf.setInt("nrows", 12001);
		conf.setStrings("cellsize", "0.0008333");
		String[] ioArgs = new String[] {"/user/hadoop/Gradient/in", "/user/hadoop/Gradient/out"};
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		
		if(otherArgs.length !=2) {
			System.err.println("Usage:Gradient <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Gradient");
		job.setJarByClass(Gradient.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,  new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
