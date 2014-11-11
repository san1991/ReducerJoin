package com.company;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by San on 11/10/2014.
 */
public class TestReadIn {
    public static void main(String[] args) {
        HashMap<String, String> lookupTable = new HashMap<String, String>();
        try{
            BufferedReader br = new BufferedReader(new FileReader("test.txt"));
            try{
                String line;
                line = br.readLine();
                while(line != null){
                    String[] parts = line.split("-");
                    //String[] num = parts[1].split(" ");
                    //ArrayList<Integer> numList = new ArrayList<Integer>();
                    //for(String x : num){
                    //    numList.add(Integer.parseInt(x));
                    //}
                    lookupTable.put(new String(parts[0]), new String(parts[1]));
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
        Set set = lookupTable.entrySet();

        Iterator i = set.iterator();
        while(i.hasNext()){
            Map.Entry me = (Map.Entry) i.next();
            System.out.println(me.getKey() + ": " + me.getValue());
        }
    }
}
