/*Reference:
1. http://codereview.stackexchange.com/questions/42885/calculate-min-max-average-and-variance-on-a-large-dataset
*/	 
import java.io.IOException;

	    import org.apache.hadoop.fs.FileSystem;
	    import org.apache.hadoop.fs.Path;
	    import org.apache.hadoop.conf.*;
	    import org.apache.hadoop.io.*;
	    import org.apache.hadoop.mapreduce.*;
	    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	    import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
	    import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	    import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

	    public class stat {
	        

	        public static class statmap  extends
	                Mapper<LongWritable, Text, Text, MapWritable> {
	            Text c = new Text("Total Numbers");
	            Text maxi = new Text("Maximum - ");
	            Text mini = new Text("Minimum - ");
	            Text totalsum = new Text("Sum - ");
	            Text text = new Text("1");

	            MapWritable mw = new MapWritable();

	            @Override
	            protected void map(LongWritable key, Text value, Context context)
	                    throws IOException, InterruptedException {
	                String line = value.toString();
	                int number = Integer.parseInt(line);
	           //     IntWritable num = new IntWritable(number);

	                mw.put(c, new IntWritable(1));
	                mw.put(maxi, new IntWritable(number));
	                mw.put(mini, new IntWritable(number));
	                mw.put(totalsum, new IntWritable(number));

	                context.write(text, mw);

	            }

	        }

	        public static class statreduce extends
	                Reducer<Text, MapWritable, Text, FloatWritable> {

	            Text c = new Text("Total Numbers");
	            Text maxi = new Text("Maximum - ");
	            Text mini = new Text("Minimum - ");
	            Text totalsum = new Text("Sum - ");
	            Text text = new Text("1");
	            MapWritable mw = new MapWritable();

	            @Override
	            protected void reduce(Text key, Iterable<MapWritable> values,
	                                  Context context) throws IOException, InterruptedException {

	                MapWritable firstMapWritable = values.iterator().next();
	                int max = ((IntWritable) firstMapWritable.get(maxi)).get();
	                int min = ((IntWritable) firstMapWritable.get(mini)).get();
	                int count = ((IntWritable) firstMapWritable.get(c)).get();
	                int number = ((IntWritable) firstMapWritable.get(totalsum)).get();

	                int sum = number;

	                int mean = 0;
	                int M2 = 0;
	                int delta = number - mean;
	                mean = mean + delta / count;
	                M2 += delta * (number - mean);

	                for (MapWritable m:values) {
	                    IntWritable sumWritable = (IntWritable) m.get(totalsum);
	                    IntWritable countIntWritable = (IntWritable) m.get(c);
	                    
	                    delta = number - mean;
	                    mean = mean + delta / count;
	                    M2 += delta * (number - mean);

	                    count += countIntWritable.get();

	                    IntWritable maxWritable = (IntWritable) m.get(maxi);
	                    max = Math.max(maxWritable.get(), max);

	                    IntWritable minWritable = (IntWritable) m.get(mini);
	                    min = Math.min(minWritable.get(), min);

	                    number = sumWritable.get();
	                    sum += number;
	                }
	                
	                context.write(c, new FloatWritable(count));
	                context.write(maxi, new FloatWritable(max));
	                context.write(mini, new FloatWritable(min));
	                context.write(totalsum, new FloatWritable(sum));
	                float finalMean = (float) sum / count;
	                context.write(new Text("Mean - "), new FloatWritable(finalMean));

	                //Standard Deviation = square root of Variance
	                double std = Math.sqrt((float) M2 / (count - 1));
	                double variance = std*std;
	                context.write(new Text("Variance - "), new FloatWritable((float) variance));
	                context.write(new Text("Standard Deviation - "), new FloatWritable((float) std));


	            }
	        }


	        public static void main(String[] args) throws IllegalArgumentException,
	                IOException, ClassNotFoundException, InterruptedException {

	        	Path ip =  new Path(args[0]);
	        	Path op = new Path(args[1]);
	        	
	    		Configuration conf = new Configuration();

	    		Job job = new Job(conf, "stats");

	    		job.setJarByClass(stat.class);

	    		job.setOutputKeyClass(Text.class);
	    		job.setOutputValueClass(MapWritable.class);

	    		job.setMapperClass(statmap.class);
	    		job.setReducerClass(statreduce.class);

	    		job.setInputFormatClass(TextInputFormat.class);
	    		job.setOutputFormatClass(TextOutputFormat.class);

	    		job.setNumReduceTasks(1);

	    		FileInputFormat.addInputPath(job,ip);
	    		FileOutputFormat.setOutputPath(job,op);

	    		// Deleting output if it exists
	    		FileSystem hdfs = FileSystem.get(conf);
	    		if (hdfs.exists(op))
	    			hdfs.delete(op, true);

	    		job.waitForCompletion(true);
	        }
	    }


