package com.company;

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

public class ReduceJoin {

    //WritaleComparable interface used to wrap the key
    public class TaggedKey implements Writable, WritableComparable<TaggedKey>{
        private Text joinKey = new Text(); //a string
        private IntWritable tag = new IntWritable(); //int: either a 1 or a 2

        @Override
        public int compareTo(TaggedKey taggedKey){
            int compareValue = this.joinKey.compareTo(taggedKey.getJoinKey());
            if(compareValue == 0){
                compareValue = this.tag.compareTo(taggedKey.getTag());
            }
            return compareValue;
        }
        //when TaggedKey class is sorted, keys with the same joinKey value will have
        //a secondary sort on the value of the 'tag' field, ensuring the order we want.
    }

    //a custom partitioner that only consider the join key when determining which reducer to send to
    public class TaggedJoiningPartitioner extends Partitioner<TaggedKey, Text>{
        @Override
        public int getPartition(TaggedKey taggedKey, Text text, int numPartitions){
            return taggedKey.getJoinKey().hashCode()%numPartitions;
        }
    }

    //A group comparator: consider only the join key when deciding how to group the values
    public class TaggedJoiningGroupingComparator extends WritableComparator{
        public TaggedJoiningGroupingComparator(){
            super(TaggedKey.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b){
            TaggedKey taggedKey1 = (TaggedKey) a;
            TaggedKey taggedKey2 = (TaggedKey) b;
            return taggedKey1.getJoinKey().compareTo(taggedKey2.getJoinKey());
        }
    }

    //Mapper
    public class JoiningMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {
        private int keyIndex;
        private Splitter splitter;
        private Joiner joiner;
        private TaggedKey taggedKey = new TaggedKey();
        private Text data = new Text();
        private int joinOrder;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            keyIndex = Integer.parseInt(context.getConfiguration().get("keyIndex"));
            String separator = context.getConfiguration().get("separator");
            splitter = Splitter.on(separator).trimResults();
            joiner = Joiner.on(separator);
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            joinOrder = Integer.parseInt(context.getConfiguration().get(fileSplit.getPath().getName()));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<String> values = Lists.newArrayList(splitter.split(value.toString()));
            String joinKey = values.remove(keyIndex);
            String valuesWithOutKey = joiner.join(values);
            taggedKey.set(joinKey, joinOrder);
            data.set(valuesWithOutKey);
            context.write(taggedKey, data);
        }

    }
    public static void main(String[] args) {
	// write your code here
    }
}
