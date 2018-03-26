import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.hash.Hash;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

// Do not change the signature of this class
public class TextAnalyzer extends Configured implements Tool {

    // Replace "?" with your own output key / value types
    // The four template data types are:
    //     <Input Key Type, Input Value Type, Output Key Type, Output Value Type>
    public static class TextMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            // Implementation of you mapper function

            String[] valueArr = value.toString().toLowerCase().split("\\W+");
            for (String contextWord : valueArr) {
                MapWritable result = new MapWritable();
                int max = 0; // keep a track of max somewhere
                for(String word : valueArr) {

                    Text wordText = new Text(word);
                    int count = result.containsKey(wordText) ? Integer.parseInt(result.get(wordText).toString()) : 0;
                    result.put(new Text(word), new Text(String.valueOf(count + 1)));
                    if(count+1 > max) max = count + 1;
                }
                Text mapConWord = new Text(contextWord);
                context.write(mapConWord, result);
            }
        }
    }

    // Replace "?" with your own key / value types
    // NOTE: combiner's output key / value types have to be the same as those of mapper
//    public static class TextCombiner extends Reducer<?, ?, ?, ?> {
//        public void reduce(Text key, Iterable<Tuple> tuples, Context context)
//            throws IOException, InterruptedException
//        {
//            // Implementation of you combiner function
//        }
//    }

    // Replace "?" with your own input key / value types, i.e., the output
    // key / value types of your mapper function
    public static class TextReducer extends Reducer<Text, MapWritable, Text, Text> {
        private final static Text emptyText = new Text("");

        public void reduce(Text key, Iterable<MapWritable> queryTuples, Context context)
            throws IOException, InterruptedException
        {
            Map<String, Integer> map = new TreeMap<>();

            // Implementation of you reducer function

            // Write out the results; you may change the following example
            // code to fit with your reducer function.
            //   Write out the current context key
            context.write(key, emptyText);
            //   Write out query words and their count
            for(String queryWord: map.keySet()){
                String count = map.get(queryWord).toString() + ">";
                Text queryWordText = new Text();
                queryWordText.set("<" + queryWord + ",");
                context.write(queryWordText, new Text(count));
            }
            //   Empty line for ending the current context key
            context.write(emptyText, emptyText);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "EID1_EID2"); // Replace with your EIDs
        job.setJarByClass(TextAnalyzer.class);

        // Setup MapReduce job
        job.setMapperClass(TextMapper.class);
        //   Uncomment the following line if you want to use Combiner class
        // job.setCombinerClass(TextCombiner.class);
        job.setReducerClass(TextReducer.class);

        // Specify key / value types (Don't change them for the purpose of this assignment)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //   If your mapper and combiner's  output types are different from Text.class,
        //   then uncomment the following lines to specify the data types.
        //job.setMapOutputKeyClass(?.class);
        //job.setMapOutputValueClass(?.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // Do not modify the main method
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TextAnalyzer(), args);
        System.exit(res);
    }

    // You may define sub-classes here. Example:
    // public static class MyClass {
    //
    // }
}



