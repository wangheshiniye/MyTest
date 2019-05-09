package com.tjdata.mr.main;

import com.tjdata.mr.tools.BaseDataLoader;
import com.tjdata.mr.tools.PropertiesReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

public class PreprocessFocusMain {

    public static void main(String args[]) throws Exception {
        Configuration conf = new Configuration();
        String[] initArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        System.out.println("initArgs = " + Arrays.asList(initArgs));
        if (initArgs.length != 5) {
            System.err.println("Usage: <input prov_id> <input day_id> <input time> <input flag>  <input configdir>");
            System.exit(-1);
        }
        PropertiesReader.init(initArgs[4]);
        BaseDataLoader.setLog4jConfig();
        conf.set("mapreduce.job.queuename", PropertiesReader.getConfig("mapreduce.job.queuename", "root.vendor.ven45"));
        conf.set("mapreduce.job.priority", PropertiesReader.getConfig("mapreduce.job.priority", "HIGH"));
        conf.set("mapreduce.map.memory.mb", PropertiesReader.getConfig("mapreduce.map.memory.mb", "4096"));
        conf.set("mapreduce.map.cpu.vcores", PropertiesReader.getConfig("mapreduce.map.cpu.vcores", "5"));
        conf.set("mapreduce.reduce.memory.mb", PropertiesReader.getConfig("mapreduce.reduce.memory.mb", "4096"));
        conf.set("mapreduce.reduce.cpu.vcores", PropertiesReader.getConfig("mapreduce.reduce.cpu.vcores", "5"));
        conf.set("mapred.reduce.tasks", PropertiesReader.getConfig("mapred.reduce.tasks", "10"));
        conf.set("map.failures.maxpercent", PropertiesReader.getConfig("map.failures.maxpercent", "5"));
        conf.set("mapreduce.reduce.failures.maxpercent", PropertiesReader.getConfig("mapreduce.reduce.failures.maxpercent", "5"));
        String flag = initArgs[3];
        if (flag.equals("0")) {
            System.out.println("main running...");
            int status = ToolRunner.run(conf, new PreprocessFilterMapReduce(), args);
            System.out.println("main finished...");
            System.exit(status);
        } else {
            System.out.println("data process running...");
            //删除历史数据
            BaseDataLoader.clearHis();
            System.out.println("data process finished...");
            System.exit(0);
        }
    }

}
