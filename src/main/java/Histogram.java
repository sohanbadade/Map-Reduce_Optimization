import java.io.*;
import java.util.*;
import java.util.Vector;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


/* single color intensity */
class Color implements WritableComparable<Color> {
    public short type;       /* red=1, green=2, blue=3 */
    public short intensity;  /* between 0 and 255 */
    /* need class constructors, toString, write, readFields, and compareTo methods */
    public Color () {}

    public Color (String str) {
        String[] st= str.split(" ");
        String str1 = st[0];
        String str2 = st[1];

        type = Short.parseShort(str1);
        intensity = Short.parseShort(str2);

    }

    public Color ( short t, short i ) {
        type = t;
        intensity = i;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeShort(type);
        out.writeShort(intensity);
    }

    public void readFields ( DataInput in ) throws IOException {
        type = in.readShort();
        intensity = in.readShort();
    }

 public String toString()
    {
        return type+" "+intensity;
    }


       public int compareTo(Color obj) {
       int cmp = Short.compare(type, obj.type);
       if(cmp!=0)
       {
        return cmp;
       }
       return Short.compare(intensity, obj.intensity);
     }
}


public class Histogram extends Configured implements Tool {


        @Override
        public int run ( String [] args ) throws Exception {

        Configuration conf = getConf();
                Job job = Job.getInstance(conf, "job1");
        job.setJarByClass(Histogram.class);
        job.setJobName("assignment2_Histogram");
        job.setMapOutputKeyClass(Color.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Histogram.HistogramMapper.class);
        job.setCombinerClass(Histogram.HistogramCombiner.class);
        job.setReducerClass(Histogram.HistogramReducer.class);
        job.setInputFormatClass(TextInputFormat.class );
        job.setOutputFormatClass(TextOutputFormat.class);
        job.waitForCompletion(true);

                Job job2 = Job.getInstance(conf, "job2");



        job2.setJarByClass(Histogram.class);
        job2.setJobName("assignment2_Histogram");
        job2.setMapOutputKeyClass(Color.class);
        job2.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "2"));
        job2.setMapperClass(Histogram.HistogramInMapCombine.class);
        job2.setReducerClass(Histogram.HistogramReducer.class);
        job2.setInputFormatClass(TextInputFormat.class );
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.waitForCompletion(true);



        return 0;
    }
   public static class HistogramInMapCombine extends Mapper<Object,Text,Color,IntWritable> {

            public Map<String, Long> map;

        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            /* write your mapper code */
            Map<String, Long> map = getMap();
            long one =  1;
            String[] colors= value.toString().split(",");

            for (int j =0; j<colors.length; j++)
                {
                    String attach = Integer.toString(j+1);
                String colorString = attach +" "+colors[j];

                if(map.containsKey(colorString)){
                    long total = map.get(colorString);
                    map.put(colorString, total+one);
                }
                else{
                    map.put(colorString, one);
                }
            }
        }


     protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<String, Long> map = getMap();
            Iterator<Map.Entry<String, Long>> it = map.entrySet().iterator();

            while(it.hasNext()) {
            Map.Entry<String, Long> entry = it.next();
            String colorString = entry.getKey();
            int total = entry.getValue().intValue();
            context.write(new Color(colorString), new IntWritable(total));
            }
        }


            public Map<String, Long> getMap() {
            if(null == map) //lazy loading
            map = new HashMap<String, Long>();
            return map;
        }
    }

    public static class HistogramMapper extends Mapper<Object,Text,Color,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            /* write your mapper code */
            String colors[] = value.toString().split(",");
            for (int j =0; j<colors.length; j++)
            {
                Color color =new Color((short)(j+1), Short.valueOf(colors[j]));
                context.write(color, new IntWritable (1));
            }
        }
    }

    public static class HistogramCombiner extends Reducer<Color,IntWritable,Color,IntWritable> {
        @Override
        public void reduce ( Color key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {

                IntWritable answer = new IntWritable();
                int sum = 0;

                for (IntWritable v : values) {
                        sum += v.get();
                }
                answer.set(sum);
                context.write(key, answer);

        }
    }

    public static class HistogramReducer extends Reducer<Color,IntWritable,Color,LongWritable> {
        @Override
        public void reduce ( Color key, Iterable<IntWritable> values, Context context )
                           throws IOException, InterruptedException {
            /* write your reducer code */
                LongWritable answer = new LongWritable();
                int sum = 0;

                for (IntWritable v : values) {
                        sum += v.get();
                }

                answer.set(sum);
                context.write(key, answer);
            }
    }

    public static void main ( String[] args ) throws Exception {
        /* write your main program code */

        ToolRunner.run(new Configuration(),new Histogram(),args );

    }
}
