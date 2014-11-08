/**
 * Created by San on 11/5/2014.
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

public class SecondarySorting {
    //first need a custom partitioner that only hash the natural key
    //if error, can do extends Partitioner
    public class SimplePartitioner implements Partitioner{
        @Override
        public int getPartition(Text compositeKey, LongWritable value, int numReduceTasks){
            //split the key into natural and augment
            String[] temp = compositeKey.toString().split(":");
            return temp[0].hashCode();
        }
    }

    //now need to tell reducer to group by the nautral key as well
    public class SimpleGroupingComparator extends WritableComparator{
        @Override
        public int compare(Text key1, Text key2){
            return compare(key1.getNaturalKey(), key2.getNaturalKey())
        }
    }
}
