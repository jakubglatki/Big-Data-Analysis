package gemma.org.Application2;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

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

public class Application2 {
    public static class CheckPhrasesOccurrences extends Mapper<Object, Text, Text, IntWritable>{


        //for the words that aren't included in the data
        private final static IntWritable none = new IntWritable(0);
        private final static IntWritable one = new IntWritable(1);

        private Text wordOccurrence = new Text("Number of words: ");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            String lengthString= context.getConfiguration().get("numberOfWords");
            int numberOfWords = Integer.parseInt(lengthString);
            ArrayList<String> wordsList = new ArrayList<String>();
            int i=0;
            while (i<numberOfWords){
                String temp = "";
                temp = context.getConfiguration().get("word"+i);
                wordsList.add(temp);
                i++;
            }

            for(int j=0;j<words.length;j++)
            {
                for(int k=0; k<wordsList.size(); k++) {
                    if (words[j].equals(wordsList.get(k))) {
                        String temp = wordsList.get(k);
                        wordOccurrence.set(temp);
                        context.write(wordOccurrence, one);
                    }

                    else
                    {
                        String temp = wordsList.get(k);
                        wordOccurrence.set(temp);
                        context.write(wordOccurrence, none);
                    }
                }
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
        //conf.set("custom_arguments_name", "custom_string");
        //conf.setInt("custom_argument_name", 10);
        if(args.length <= 2) {
            throw new RuntimeException("Need at least three arguments (input and output folders on HDFS, and words that occurrences are needed to be checked)");
        }

        Path input_folder  = new Path( args[0] );
        Path output_folder = new Path( args[1] );

        //we are starting on i=2 as it is going from third argument on
        for(int i=2;i<args.length;i++)
        {
            conf.set("word"+ (i - 2), args[i]);
        }
        conf.setInt("numberOfWords",args.length-2);


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

        job.setJarByClass(Application2.class);
        job.setMapperClass(CheckPhrasesOccurrences.class);
        job.setReducerClass(SumPhrasesOccurrences.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input_folder);
        FileOutputFormat.setOutputPath(job, output_folder);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}