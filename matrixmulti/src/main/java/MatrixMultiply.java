import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author TripleZ
 * @since 2017-11-15
 * @version 1.0
 */
public class MatrixMultiply {
    // Map.
    public static class MatrixMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        private String flag = null;// 数据集名称
        private int rowNum = 3;// 矩阵A的行数
        private int colNum = 3;// 矩阵B的列数
        private int rowIndexA = 1; // 矩阵A，当前在第几行
        private int rowIndexB = 1; // 矩阵B，当前在第几行

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            flag = ((FileSplit) context.getInputSplit()).getPath().getName();// 获取文件名称
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if ("A".equals(flag)) {
                for (int i = 1; i <= colNum; i++) {
                    Text k = new Text(rowIndexA + "," + i); // 将 A 矩阵的行列值写入 key 中
                    for (int j = 0; j < tokens.length; j++) {
                        int temp = Integer.parseInt(tokens[j]) + 1;
                        Text v = new Text("a," + (j + 1) + "," + String.valueOf(temp));
                        context.write(k, v);
                    }
                }
                rowIndexA++;// 每执行一次map方法，矩阵向下移动一行
            } else if ("B".equals(flag)) {
                for (int i = 1; i <= rowNum; i++) {
                    for (int j = 0; j < tokens.length; j++) {
                        Text k = new Text(i + "," + (j + 1));
                        int temp = Integer.parseInt(tokens[j]) + 1;
                        Text v = new Text("b," + rowIndexB + "," + String.valueOf(temp));
                        context.write(k, v);
                    }
                }
                rowIndexB++;// 每执行一次map方法，矩阵向下移动一行
            }
        }
    }

    // Reduce.
    public static class MatrixReducer extends Reducer<Text, Text, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, String> mapA = new HashMap<String, String>();
            Map<String, String> mapB = new HashMap<String, String>();

            for (Text value : values) {
                String[] val = value.toString().split(",");
                if ("a".equals(val[0])) {
                    mapA.put(val[1], val[2]);
                } else if ("b".equals(val[0])) {
                    mapB.put(val[1], val[2]);
                }
            }

            int result = 0;
            Iterator<String> mKeys = mapA.keySet().iterator();
            while (mKeys.hasNext()) {
                String mkey = mKeys.next();
                if (mapB.get(mkey) == null) {// 因为 mkey 取的是 mapA 的 key 集合，所以只需要判断 mapB 是否存在即可。
                    continue;
                }
                result += Integer.parseInt(mapA.get(mkey))
                        * Integer.parseInt(mapB.get(mkey));
            }
            context.write(key, new IntWritable(result));
        }
    }

    // Main function.
    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Matrix Multiply");
        job.setJarByClass(MatrixMultiply.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}