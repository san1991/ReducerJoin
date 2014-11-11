package com.company;

/**
 * Created by San on 11/10/2014.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class PartitionJoin {

    public static class PartitionMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        //initiate a look up table accessible for all
        private HashMap lookupTable = new HashMap();

        //the setup function will be called only once to populate the lookupTable
        public void setup(org.apache.hadoop.mapreduce.Mapper.Context context) throws IOException, InterruptedException{
            try{
                Path path = new Path("/input/lookupTable.txt");
                FileSystem  fs = FileSystem.get(new Configuration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

                try{
                    String line;
                    line = br.readLine();
                    while(line != null){
                        String[] parts = line.split("-");
                        String[] num = parts[1].split(" ");
                        ArrayList numList = new ArrayList();
                        for(String x : num){
                            numList.add(Integer.parseInt(x));
                        }
                        lookupTable.put(new String(parts[0]), new ArrayList(numList));
                        line = br.readLine();
                    }
                } catch(IOException e){
                    System.out.println("Exception while populating lookup table");
                    e.printStackTrace();
                }
                finally{
                    br.close();
                }
            } catch (Exception e){
                System.out.println("Exception while reading the lookup table");
                e.printStackTrace();
            }
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
               	output.collect(word, one);
            }
        }
    }

            	   public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
           public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
                   int sum = 0;
            	       while (values.hasNext()) {
              	         sum += values.next().get();
               	       }
                   output.collect(key, new IntWritable(sum));
           	     }
       	   }

               public static void main(String[] args) throws Exception {
        	     JobConf conf = new JobConf(WordCount.class);
             conf.setJobName("wordcount");

      	     conf.setOutputKeyClass(Text.class);
        	     conf.setOutputValueClass(IntWritable.class);

        	     conf.setMapperClass(Map.class);
             conf.setCombinerClass(Reduce.class);
             conf.setReducerClass(Reduce.class);

        	     conf.setInputFormat(TextInputFormat.class);
        	     conf.setOutputFormat(TextOutputFormat.class);

        	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
        	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));

       	     JobClient.runJob(conf);
          }
    	}