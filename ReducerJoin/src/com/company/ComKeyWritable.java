package com.company;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by San on 11/4/2014.
 */
public class ComKeyWritable implements WritableComparable<ComKeyWritable>{

    private String key;
    private String tag;

    @Override
    public void readFields(DataInput in) throws IOException{
        key = in.readUTF();
        tag = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException{
        out.writeUTF(key);
        out.writeUTF(tag);
    }

    @Override
    public int compareTo(ComKeyWritable o){
        return ComparisonChain.start().compare(key, o.key).compare(tag).compare(tag, o.tag).result();
    }
}
