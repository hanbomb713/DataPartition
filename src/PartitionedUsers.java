import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class PartitionedUsers {

	   public static class CreationDateMapper extends
	         Mapper<Object, Text, IntWritable, Text> {

	      // This object will format the creation date string into a Date object
	      private final static SimpleDateFormat frmt = new SimpleDateFormat(
	            "yyyy-MM-dd'T'HH:mm:ss.SSS");

	      private IntWritable outkey = new IntWritable();

	      @Override
	      protected void map(Object key, Text value, Context context)
	            throws IOException, InterruptedException {

	         // Parse the input user data into a nice map
	         Map<String, String> parsed = MRDPUtil.transformXmlToMap(value
	               .toString());

	         String strDate = parsed.get("CreationDate");

	         // skip this record if date is null
	         if (strDate != null) {
	            try {
	               // Parse the string into a Calendar object
	               Calendar cal = Calendar.getInstance();
	               cal.setTime(frmt.parse(strDate));
	               // The key will be year like 2010
	               outkey.set(cal.get(Calendar.YEAR));
	               // Write out the year with the input value
	               context.write(outkey, value);
	            } catch (java.text.ParseException e) {
	               // An error occurred parsing the creation Date string
	               // skip this record
	            }
	         }
	      }
	   }
	   public static class CreationDatePartitioner extends
	         Partitioner<IntWritable, Text> implements Configurable {

	      private static final String MIN_CREATION_DATE_YEAR = "min.creation.date.year";

	      private Configuration conf = null;
	      private int minCreationDateYear = 0;

	      @Override
	      public int getPartition(IntWritable key, Text value, int numPartitions) {
	         // E.g., partition ID = key.get() - 2008 = 2010 - 2008 = 2
	         return (key.get() - minCreationDateYear)%numPartitions;
	      }

	      public Configuration getConf() {
	         return conf;
	      }

	      public void setConf(Configuration conf) {
	         this.conf = conf;
	         // In this example, the value of minLastAccessDateYear is 2008
	         minCreationDateYear = conf.getInt(MIN_CREATION_DATE_YEAR, 0);
	      }

	      public static void setMinLastAccessDate(Job job,
	            int minCreationDateYear) {
	         // In this example, the value of minLastAccessDateYear is 2008
	         job.getConfiguration().setInt(MIN_CREATION_DATE_YEAR,minCreationDateYear);
	      }
	   }
	   public static class ValueReducer extends
       Reducer<IntWritable, Text, Text, NullWritable> {

    protected void reduce(IntWritable key, Iterable<Text> values,
          Context context) throws IOException, InterruptedException {
       for (Text t : values) {
          context.write(t, NullWritable.get());
       }
    }
 }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args)
          .getRemainingArgs();
    if (otherArgs.length != 2) {
       System.err.println("Usage: PartitionedUsers <users> <outdir>");
       System.exit(2);
    }

    Job job = new Job(conf, "Partitionusers");
    job.setJarByClass(PartitionedUsers.class);
    job.setMapperClass(CreationDateMapper.class);

    // Set custom partitioner and minimum last access date
    job.setPartitionerClass(CreationDatePartitioner.class);
    CreationDatePartitioner.setMinLastAccessDate(job, 2008);

    // creation dates span between 2008-2013, or 6 years
    job.setNumReduceTasks(6);
    job.setReducerClass(ValueReducer.class);

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    job.setOutputFormatClass(TextOutputFormat.class);
    job.getConfiguration().set("mapred.textoutputformat.separator", "");

    System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}