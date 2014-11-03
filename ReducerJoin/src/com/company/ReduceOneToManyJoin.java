package com.company;

/**
 * Created by Shen on 11/2/2014.
 */

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.*;

public class ReduceOneToManyJoin {
    public static class Map extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, Text> {
        private Text keyAttribute = new Text();
        private Text remainingValue = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
                        Reporter reporter) throws IOException{
            String line = value.toString();

            //split the line into key and value
            int i = line.indexOf(' ');
            keyAttribute.set(line.substring(0,i));
            remainingValue.set(line.substring(i));

            //form the key value pair
            output.collect(keyAttribute, remainingValue);
        }
    }
}
