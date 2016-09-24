package study;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Allocation {
	
	public static class Map extends Mapper<Object, Text, IntWritable, Text> {                 
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString(); 
			//处理表头
			if (line.contains("xllcorner") || line.contains("yllcorner") || line == null || line.equals("") || line.contains("NODATA_value")
					|| line.contains("cellsize") || line.contains("ncols") || line.contains("nrows")) {
					return;
			}			
			Configuration conf = context.getConfiguration();  
            int ncols = conf.getInt("ncols", 0);
            int nrows = conf.getInt("nrows", 0);	
			String[] a =line.split(" ");
			for(int i=1; i<=ncols; i++) {
				//找出所有点的位置
				if((!a[i].equals("-9999"))) {
					for(int j=1; j<=nrows; j++) {
						context.write(new IntWritable(j), new Text(a[i] + ":" + a[0] + ":" + i));
					}
				} 
			}
		}
	}
	

	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce (IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {	
			Configuration conf = context.getConfiguration(); 
			int ncols = conf.getInt("ncols", 0);
			int row = key.get();
			StringBuffer valueInfo = new StringBuffer();
			Vector<int[]> points = new Vector<int[]>();		
			for(Text value : values) {
				String v = value.toString();
				//找出每个目标点的位置放入points，找出每行点的个数num
				String[] p = v.split(":"); 
				int[] point = {Integer.parseInt(p[0]), Integer.parseInt(p[1]), Integer.parseInt(p[2])};
				points.add(point);
			}	
			//每行中每个点依次与目标点求距离，取最小值
			for(int i=1; i<=ncols; i++) {			
				double distance[] = new double[2];
				distance[0] = 999;
				distance[1] = 1000000;
				int xa = row;
				int ya = i;
				for(int[] p : points) {
					int xb = p[1];
					int yb = p[2];
					double tmp = Math.sqrt(Math.pow((xa-xb), 2) + Math.pow((ya-yb), 2));
					if(tmp < distance[1]) {
						distance[1] = tmp;
						distance[0] =	p[0]; 
					}
				}
				valueInfo.append(distance[0] + " ");				
			}
			context.write(null, new Text(valueInfo.toString()));
		}
	}
	
	public void Mapreduce() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://121.48.175.121:9000");
		String[] ioArgs = new String[] {"/LPW/Allocation/in", "/LPW/Allocation/out"};
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();		
		if(otherArgs.length !=2) {
			System.err.println("Usage:Allocation <in> <out>");
			System.exit(2);
		}		
		conf.setInt("ncols", 12001);
		conf.setInt("nrows", 12001);
		conf.setStrings("cellsize", "0.0008333");
		Job job = new Job(conf, "Allocation");
		job.setJarByClass(Allocation.class);		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,  new Path(otherArgs[1]));
		if (job.waitForCompletion(true)) {
			System.out.println("MapReduce执行完毕！");
			System.exit(0);
		}
	}	
	
	
	public static void main(String[] args) throws Exception {
		Allocation allo = new Allocation();
		allo.Mapreduce();
	}
}
