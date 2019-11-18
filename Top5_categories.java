package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Top5_categories extends Configured implements Tool {


 public static void main(String[] args) throws Exception {
 int res = ToolRunner.run(new Top5_categories(), args);
 System.exit(res);
 }

 public int run(String[] args) throws Exception {
 Job job = Job.getInstance(getConf(), "Top5_categories");
 job.setJarByClass(this.getClass());
 // Use TextInputFormat, the default unless job.setInputFormatClass is used
 FileInputFormat.addInputPath(job, new Path(args[0]));
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 job.setMapperClass(Map.class);
 job.setReducerClass(Reduce.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(IntWritable.class);
 return job.waitForCompletion(true) ? 0 : 1;
 }


   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
      private Text category = new Text();
      private final static IntWritable one = new IntWritable(1);
      public void map(LongWritable key, Text value, Context context )
        throws IOException, InterruptedException {
           String line = value.toString();
           String str[]=line.split("\t");
          if(str.length > 5){
                category.set(str[3]);
}
      context.write(category, one);
}
}

public static class Reduce extends Reducer<Text, IntWritable,Text,IntWritable>{
   public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
           int sum = 0;
           for (IntWritable val : values) {
               sum += val.get();
}
           context.write(key, new IntWritable(sum));
}
}

}
