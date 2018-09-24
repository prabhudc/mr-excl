import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.IOException;
import java.util.*;

public class exclusions extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new exclusions(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("./output/");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Find exclusive followers");
        jobA.setMapOutputKeyClass(IntWritable.class);
        jobA.setMapOutputValueClass(Text.class);

        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);


        jobA.setMapperClass(MapClass.class);
        jobA.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(jobA, new Path("./input.txt"));
        FileOutputFormat.setOutputPath(jobA, tmpPath);


        return jobA.waitForCompletion(true) ? 0 : 1;

    }


    public static class MapClass extends Mapper<Object, Text, IntWritable, Text> {


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            ArrayList<String> connection = new ArrayList<>();
            String output = new String();

            String[] strList = line.split(",");
            connection.add(strList[0]);
            connection.add(strList[1]);

            for(String v : connection){
                output += "~" + v;
            }

            if (strList.length == 2)
                context.write(new IntWritable(1), new Text(output));

        }
    }




    public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] str = new String[3];
            Set<String> packFollower = new HashSet<>();
            ArrayList<String > arrPackFollower =   new ArrayList<>();
            Map<String,String> others = new HashMap<>();


            for(Text v : values){

                str =   v.toString().split("~");

                if(str[2].equals("@Packers")){
                    packFollower.add(str[1]);
                }else{
                    others.put(str[1] + "~" + str[2], null);
                }
            }

            arrPackFollower.addAll(packFollower);


            for (int i = 0; i < arrPackFollower.size(); i++) {
                for (int j = i + 1; j < arrPackFollower.size(); j++) {

                    if( !others.containsKey(arrPackFollower.get(i) + "~" + arrPackFollower.get(j)) && !others.containsKey(arrPackFollower.get(j) + "~" + arrPackFollower.get(i))){
                        context.write(new Text(arrPackFollower.get(i)), new Text(arrPackFollower.get(j)));
                    }
                }

            }




        }
    }


}
