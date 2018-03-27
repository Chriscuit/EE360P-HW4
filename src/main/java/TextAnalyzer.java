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

import java.io.IOException;

import java.util.*;

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

            if(!value.toString().equals("")) {

                HashSet<String> hashSet = new HashSet<>();
                String[] valueArr = value.toString().toLowerCase().split("\\W+");

                for (String contextWord : valueArr) {
                    if(!contextWord.equals("") && !hashSet.contains(contextWord)) {

                        hashSet.add(contextWord);
                        MapWritable result = new MapWritable();
                        for (String word : valueArr) {
                            if (!word.equals("")) {
                                Text wordText = new Text(word);
                                int count = result.containsKey(wordText) ? Integer.parseInt(result.get(wordText).toString()) : 0;
                                result.put(wordText, new Text(String.valueOf(count + 1)));
                            }
                        }
                        Text mapConWord = new Text(contextWord);

                        if(Integer.parseInt(result.get(mapConWord).toString()) == 1) {
                            result.remove(mapConWord);
                        } else {
                            result.put(mapConWord, new Text(String.valueOf(Integer.parseInt(result.get(mapConWord).toString()) - 1)));
                        }
                        context.write(mapConWord, result);
                    }
                }
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
            Map<Writable, Integer> map = new TreeMap<>();

            int max = 0;
            String top = "";
            int totalValue;

            for(MapWritable oneMap : queryTuples) {
                for(Writable word : oneMap.keySet()) {

                    int count = map.containsKey(word) ? Integer.parseInt(map.get(word).toString()) : 0;
                    totalValue = count + Integer.parseInt(String.valueOf(oneMap.get(word)));
                    map.put(word, totalValue);
                    if(totalValue > max) {
                        max = totalValue;
                        if(word.toString().compareTo(top) > 0) {
                            top = word.toString();
                        }
                    }
                }
            }

            // Implementation of you reducer function

            // Write out the results; you may change the following example
            // code to fit with your reducer function.
            //   Write out the current context key
            //   Write out query words and their count

            context.write(key, new Text(String.valueOf(max)));

            String topOutput = String.valueOf(max) + ">";
            Text topQueryWord = new Text("<" + top + ",");
            context.write(topQueryWord, new Text(topOutput));

            for(Writable queryWord: map.keySet()){

                if(!queryWord.toString().equals(top)) {
                    String count = map.get(queryWord).toString() + ">";
                    Text queryWordText = new Text();
                    queryWordText.set("<" + queryWord + ",");
                    context.write(queryWordText, new Text(count));
                }
            }
            //   Empty line for ending the current context key
            context.write(emptyText, emptyText);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "crg2957_sik269"); // Replace with your EIDs
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
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);

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



