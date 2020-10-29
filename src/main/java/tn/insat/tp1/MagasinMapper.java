package tn.insat.tp1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MagasinMapper
        extends Mapper<Object, Text, Text, FloatWritable>{

    private static FloatWritable cost = new FloatWritable();
    private Text word = new Text();

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        String magasin = value.toString().split("\t")[2];
            cost.set(Float.parseFloat(value.toString().split("\t")[4]));
            word.set(magasin);
            context.write(word, cost);

    }
}
