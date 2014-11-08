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

public class SecondarySortingTwo{
    //the composite key comparator is where the sed. sorting takes place
    //it compares key by symboll ascendingly and timestamp descendingly
    public class CompositeKeyComparator extends WritableComparator{
        protected CompositeKeyComparator(){
            super(StockKey.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        pubilc int compare(WritableComparable w1, WritableComparable w2){
            StockKey k1 = (StockKey)w1;
            StockKey k2 = (StockKey)w2;
            int result = k1.getSymbol().compareTo(k2.getSymbol());
            if(0 == result) {
                result = -1* k1.getTimestamp().compareTo(k2.getTimestamp());
            }
            return result;
        }
    }
}
