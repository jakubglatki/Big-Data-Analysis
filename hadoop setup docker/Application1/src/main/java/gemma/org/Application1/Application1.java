package gemma.org.Application1;

  
import java.io.IOException;  
import java.util.StringTokenizer;  
  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.conf.Configuration;  
  
public class Application1 {
    public static class CheckPhrasesOccurrences extends Mapper<Object, Text, Text, IntWritable>{
  
        private final static IntWritable one = new IntWritable(1);  
  
        private Text phraseOccurrences = new Text("Phrase occurrence: ");
  
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            String lengthString= context.getConfiguration().get("length_of_phrase");
            int phraseLength = Integer.parseInt(lengthString);
            int i=0;
            while (i<words.length){
                String temp = "";
                for (int j = i; j < (i+phraseLength) && j < (words.length);  j++) {
                    temp += words[j] + " ";
                }
                phraseOccurrences.set(temp);
                context.write(phraseOccurrences,one);
                i++;
            }
        }  
    }  
  
    public static class SumPhrasesOccurrences extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
  
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {  
            int sum = 0;  
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);  
        }  
    }  
  
    public static void main(String[] args) throws Exception {  
  
        Configuration conf = new Configuration();  
        conf.set("mapred.job.queue.name", "default");
        conf.setInt("length_of_phrase", Integer.parseInt(args[2]));
        //conf.set("custom_arguments_name", "custom_string");
        //conf.setInt("custom_argument_name", 10);

        if(args.length != 3) {
            throw new RuntimeException("Need three arguments (input and output folders on HDFS, and length of phrase)");
        }

        Path input_folder  = new Path( args[0] );
        Path output_folder = new Path( args[1] );
  
        // configuration should contain reference to your namenode
        FileSystem fs = FileSystem.get(new Configuration());  
        // true stands for recursively deleting the folder you gave
        if(!fs.exists(input_folder)) {
            throw new RuntimeException("Input folder does not exist on HDFS filesystem");
        }

        if(fs.exists(output_folder)) {
            //throw new RuntimeException("Output folder already exist on HDFS filesystem");
            fs.delete(output_folder, true);
        }

        Job job = Job.getInstance(conf, "Counting of the occurrences of the phrases in the text corpus");
  
        job.setJarByClass(Application1.class);
        job.setMapperClass(CheckPhrasesOccurrences.class);
        job.setReducerClass(SumPhrasesOccurrences.class);
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(IntWritable.class);  
  
        FileInputFormat.addInputPath(job, input_folder);
        FileOutputFormat.setOutputPath(job, output_folder);
  
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }  
}
