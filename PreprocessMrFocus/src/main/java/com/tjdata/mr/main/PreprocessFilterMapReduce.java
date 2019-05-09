package com.tjdata.mr.main;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tjdata.mr.tools.BaseDataLoader;
import com.tjdata.mr.tools.MyClassLoader;
import com.tjdata.mr.tools.PropertiesReader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashSet;

public class PreprocessFilterMapReduce extends Configured implements Tool {

    public static class PreprocessFilter4GMapper extends Mapper<Object, Text, NullWritable, Text> {

        private final static Logger log = LoggerFactory.getLogger(PreprocessFilter4GMapper.class);

        private MultipleOutputs<NullWritable, Text> mos = null;
        private String industryCodes = null;
        private String prov_id = null;
        private String day_id = null;
        private String time = null;
        private Object rl = null;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            this.mos = new MultipleOutputs<>(context);
            this.industryCodes = DefaultStringifier.load(context.getConfiguration(), "industryCodes", Text.class).toString();
            this.prov_id = DefaultStringifier.load(context.getConfiguration(), "prov_id", Text.class).toString();
            this.day_id = DefaultStringifier.load(context.getConfiguration(), "day_id", Text.class).toString();
            this.time = DefaultStringifier.load(context.getConfiguration(), "time", Text.class).toString();
            this.rl = MyClassLoader.invoke("rl", new Object[]{PropertiesReader.input_config_root_dir, this.industryCodes});
            super.setup(context);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            if (mos != null) {
                mos.close();
            }
            super.cleanup(context);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (!StringUtils.isBlank(line)) {
                String[] lineSplits = line.split("\\|", -1);
                if (lineSplits.length == 41 && !StringUtils.isBlank(lineSplits[1])) {
                    String[] result = MyClassLoader.invoke("log4g", new Object[]{this.rl, line});
                    if (result != null) {
                        mos.write("Text", NullWritable.get(),
                                new Text(result[1]),
                                result[0] + "/" + "tjdata-" + this.prov_id + "-" + this.day_id + this.time + "-4g");
                    }
                }
            }
        }
    }

    public static class PreprocessFilter3GMapper extends Mapper<Object, Text, NullWritable, Text> {

        private final static Logger log = LoggerFactory.getLogger(PreprocessFilter3GMapper.class);

        private MultipleOutputs<NullWritable, Text> mos = null;
        private String industryCodes = null;
        private String prov_id = null;
        private String day_id = null;
        private String time = null;
        private Object rl = null;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            this.mos = new MultipleOutputs<>(context);
            this.industryCodes = DefaultStringifier.load(context.getConfiguration(), "industryCodes", Text.class).toString();
            this.prov_id = DefaultStringifier.load(context.getConfiguration(), "prov_id", Text.class).toString();
            this.day_id = DefaultStringifier.load(context.getConfiguration(), "day_id", Text.class).toString();
            this.time = DefaultStringifier.load(context.getConfiguration(), "time", Text.class).toString();
            this.rl = MyClassLoader.invoke("rl", new Object[]{PropertiesReader.input_config_root_dir, this.industryCodes});
            super.setup(context);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            if (mos != null) {
                mos.close();
            }
            super.cleanup(context);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (!StringUtils.isBlank(line)) {
                String[] lineSplits = line.split("\\|", -1);
                if (lineSplits.length == 38 && !StringUtils.isBlank(lineSplits[1])) {
                    String[] result = MyClassLoader.invoke("log3g", new Object[]{this.rl, line});
                    if (result != null) {
                        mos.write("Text", NullWritable.get(),
                                new Text(result[1]),
                                result[0] + "/" + "tjdata-" + this.prov_id + "-" + this.day_id + this.time + "-3g");//无Cookie和地市
                    }
                }
            }
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        System.out.println("PreprocessFilterMapReduce running...");
        if (args.length < 3) {
            System.err.println("Usage: <input prov_id> <input day_id> <input time>");
            System.exit(-1);
        }
        System.out.println("args = " + Arrays.asList(args));
        String industryCodes = StringUtils.join(Sets.filter(new HashSet<>(Lists.transform(BaseDataLoader.getData("area.txt", 2), new Function<String[], String>() {
            @Override
            public String apply(String[] arg0) {
                // TODO Auto-generated method stub
                return arg0.length == 2 && arg0[1].contains(args[0]) ? arg0[0] : "";
            }
        })), new Predicate<String>() {
            @Override
            public boolean apply(String arg0) {
                // TODO Auto-generated method stub
                return StringUtils.isNotBlank(arg0);
            }
        }), ",");
        if (StringUtils.isBlank(industryCodes)) {
            System.out.println("Usage: no industry use in this province");
            return 0;
        }
        System.out.println("industryCodes = " + industryCodes);

        //设置全局变量
        Configuration conf = super.getConf();
        DefaultStringifier.store(conf, new Text(args[0]), "prov_id");
        DefaultStringifier.store(conf, new Text(args[1]), "day_id");
        DefaultStringifier.store(conf, new Text(args[2]), "time");
        DefaultStringifier.store(conf, new Text(industryCodes), "industryCodes");

        //创建任务Job
        Job job = Job.getInstance(conf, "PreprocessFilterMapReduce");
        job.setJarByClass(PreprocessFocusMain.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(PropertiesReader.getConfig("output_data_repartition_num", 5));

        //设置输出路径
        String output = PropertiesReader.getConfig("output_data_root_dir", "E:/test/mobile_log/") + "/" + args[1] + "/" + args[0] + "/" + args[2];
        Path outputPath = new Path(output);
        System.out.println("outputPath = " + outputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        MultipleOutputs.addNamedOutput(job, "Text", TextOutputFormat.class, NullWritable.class, Text.class);

        //设置输入路径
        String inputHttp4G = PropertiesReader.getConfig("input_data_root_dir", "E:/test/log/") + "dpiqixin/" + "prov_id=" + args[0] + "/day_id=" + args[1] + "/net_type=4g/" + "*" + args[1] + args[2] + "*client*";
        String inputHttps4G = PropertiesReader.getConfig("input_data_root_dir", "E:/test/log/") + "https/" + "prov_id=" + args[0] + "/day_id=" + args[1] + "/net_type=4g/protc_type=https/" + "*" + args[1] + args[2] + "*client*";
        String inputHttp3G = PropertiesReader.getConfig("input_data_root_dir", "E:/test/log/") + "dpiqixin/" + "prov_id=" + args[0] + "/day_id=" + args[1] + "/net_type=3g/" + "*" + args[1] + args[2] + "*client*";
        String inputHttps3G = PropertiesReader.getConfig("input_data_root_dir", "E:/test/log/") + "https/" + "prov_id=" + args[0] + "/day_id=" + args[1] + "/net_type=3g/protc_type=https/" + "*" + args[1] + args[2] + "*client*";
        // String inputHttp4G = PropertiesReader.getConfig("input_data_root_dir", "E:/test/log/") + "dpiqixin/" + "prov_id=" + args[0] + "/day_id=" + args[1] + "/net_type=4g/" + "*" + args[2] + "*client*";
        // String inputHttps4G = PropertiesReader.getConfig("input_data_root_dir", "E:/test/log/") + "https/" + "prov_id=" + args[0] + "/day_id=" + args[1] + "/net_type=4g/protc_type=https/" + "*" + args[2] + "*client*";
        // String inputHttp3G = PropertiesReader.getConfig("input_data_root_dir", "E:/test/log/") + "dpiqixin/" + "prov_id=" + args[0] + "/day_id=" + args[1] + "/net_type=3g/" + "*" + args[2] + "*client*";
        // String inputHttps3G = PropertiesReader.getConfig("input_data_root_dir", "E:/test/log/") + "https/" + "prov_id=" + args[0] + "/day_id=" + args[1] + "/net_type=3g/protc_type=https/" + "*" + args[2] + "*client*";
        Path inputHttp4GPath = new Path(inputHttp4G);
        Path inputHttps4GPath = new Path(inputHttps4G);
        Path inputHttp3GPath = new Path(inputHttp3G);
        Path inputHttps3GPath = new Path(inputHttps3G);
        System.out.println("inputHttp4GPath = " + inputHttp4GPath);
        System.out.println("inputHttps4GPath = " + inputHttps4GPath);
        System.out.println("inputHttp3GPath = " + inputHttp3GPath);
        System.out.println("inputHttps3GPath = " + inputHttps3GPath);

        FileSystem dfs = FileSystem.get(conf);
        FileStatus[] fileStatusHttp4G = dfs.globStatus(inputHttp4GPath);
        FileStatus[] fileStatusHttps4G = dfs.globStatus(inputHttps4GPath);
        FileStatus[] fileStatusHttp3G = dfs.globStatus(inputHttp3GPath);
        FileStatus[] fileStatusHttps3G = dfs.globStatus(inputHttps3GPath);
        String network = PropertiesReader.getConfig("input_data_network_type=4g", "all");
        String protocol = PropertiesReader.getConfig("input_data_protocol_type", "all");
        boolean http4GFlag = (network.equals("4g") || network.equals("all")) && (protocol.equals("http") || protocol.equals("all")) && fileStatusHttp4G.length > 0;
        boolean https4GFlag = (network.equals("4g") || network.equals("all")) && (protocol.equals("https") || protocol.equals("all")) && fileStatusHttps4G.length > 0;
        boolean http3GFlag = (network.equals("3g") || network.equals("all")) && (protocol.equals("http") || protocol.equals("all")) && fileStatusHttp3G.length > 0;
        boolean https3GFlag = (network.equals("3g") || network.equals("all")) && (protocol.equals("https") || protocol.equals("all")) && fileStatusHttps3G.length > 0;

        System.out.println("http4GFlag = " + http4GFlag);
        System.out.println("https4GFlag = " + https4GFlag);
        System.out.println("http3GFlag = " + http3GFlag);
        System.out.println("https3GFlag = " + https3GFlag);

        //生产环境
        if (http4GFlag) {
            MultipleInputs.addInputPath(job, inputHttp4GPath, SequenceFileAsTextInputFormat.class, PreprocessFilter4GMapper.class);
        }
        if (https4GFlag) {
            MultipleInputs.addInputPath(job, inputHttps4GPath, SequenceFileAsTextInputFormat.class, PreprocessFilter4GMapper.class);
        }
        if (http3GFlag) {
            MultipleInputs.addInputPath(job, inputHttp3GPath, SequenceFileAsTextInputFormat.class, PreprocessFilter3GMapper.class);
        }
        if (https3GFlag) {
            MultipleInputs.addInputPath(job, inputHttps3GPath, SequenceFileAsTextInputFormat.class, PreprocessFilter3GMapper.class);
        }
        //测试环境
//        if (http4GFlag) {
//            MultipleInputs.addInputPath(job, inputHttp4GPath, TextInputFormat.class, PreprocessFilter4GMapper.class);
//        }
//        if (https4GFlag) {
//            MultipleInputs.addInputPath(job, inputHttps4GPath, TextInputFormat.class, PreprocessFilter4GMapper.class);
//        }
//        if (http3GFlag) {
//            MultipleInputs.addInputPath(job, inputHttp3GPath, TextInputFormat.class, PreprocessFilter3GMapper.class);
//        }
//        if (https3GFlag) {
//            MultipleInputs.addInputPath(job, inputHttps3GPath, TextInputFormat.class, PreprocessFilter3GMapper.class);
//        }
        //执行Job
        int status = 0;
        if (http4GFlag || https4GFlag || http3GFlag || https3GFlag) {
            if (dfs.exists(outputPath)) {
               dfs.delete(outputPath, true);
            }
            status = job.waitForCompletion(true) ? 0 : 1;
        }
        return status;
    }
}
