package com.tjdata.mr.tools;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.util.*;

public class BaseDataLoader {

    public static FileSystem fs = null;

    static {
        try {
            fs = FileSystem.get(new Configuration());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<String[]> getData(String infile, int colnum) {
        List<String[]> result = new ArrayList<>();
        BufferedReader is = null;
        try {
            //InputStream inputStream = ConfigDataLoader.class.getClassLoader().getResourceAsStream(infile);
            InputStream inputStream = fs.open(new Path(PropertiesReader.getConfig("input_config_root_dir", "E:/test/config/") + infile));
            is = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
            String line;
            while ((line = is.readLine()) != null) {
                String[] splits = line.split("~", -1);
                if (splits.length != colnum) {
                    continue;
                }
                result.add(splits);
            }
        } catch (Exception ex) {
            // TODO Auto-generated catch block
            ex.printStackTrace();
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        return result;
    }

    public static Map<String, List<String[]>> getData(String infile,
                                                      int colnum, int keyIndex) {
        Map<String, List<String[]>> result = new HashMap<>();
        BufferedReader is = null;
        try {
            //InputStream inputStream = ConfigDataLoader.class.getClassLoader().getResourceAsStream(infile);
            InputStream inputStream = fs.open(new Path(PropertiesReader.getConfig("input_config_root_dir", "E:/test/config/") + infile));
            is = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
            String line;
            while ((line = is.readLine()) != null) {
                String[] splits = line.split("~", -1);
                if (splits.length != colnum || StringUtils.isBlank(splits[keyIndex])) {
                    continue;
                }
                if (!result.containsKey(splits[keyIndex])) {
                    result.put(splits[keyIndex], new ArrayList<String[]>());
                }
                result.get(splits[keyIndex]).add(splits);
            }
        } catch (Exception ex) {
            // TODO Auto-generated catch block
            ex.printStackTrace();
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        return result;
    }

    public static void saveLog(String info) {
        OutputStream os = null;
        try {
            os = fs.create(new Path(PropertiesReader.getConfig("input_config_root_dir", "E:/test/config/") + "log.txt"));
            os.write(info.getBytes());
            os.flush();
        } catch (Exception ex) {
            // TODO Auto-generated catch block
            ex.printStackTrace();
        } finally {
            if (os != null) {
                try {
                    os.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    public static void clearHis() {
        String output = PropertiesReader.getConfig("output_data_root_dir", "E:/test/mobile_log/");
        String date = DateFormatUtils.format(DateUtils.addDays(new Date(), -30), "yyyyMMdd");
        FileStatus[] fileStatus = new FileStatus[0];
        try {
            fileStatus = fs.listStatus(new Path(output));
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (FileStatus file : fileStatus) {
            Path path = file.getPath();
            String name = path.getName();
            try {
                if (new Integer(date) > new Integer(name)) {
                    fs.delete(path, true);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void setLog4jConfig() {
        InputStream is = null;
        try {
            //InputStream inputStream = ConfigDataLoader.class.getClassLoader().getResourceAsStream("log4j.properties");
            is = fs.open(new Path(PropertiesReader.getConfig("input_config_root_dir", "E:/test/config/") + "log4j.properties"));
            PropertyConfigurator.configure(is);
        } catch (Exception ex) {
            // TODO Auto-generated catch block
            ex.printStackTrace();
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        BaseDataLoader.saveLog("测试信息");
//        System.out.println(BaseDataLoader.getData("area.txt", 2));
//        System.out.println(Algorithm.invoke("rule", "X01"));
//        val urlPath = FileSystemUtils.readKey(hdfsDir + "/sys.properties", "input_data_url_root_dir", "F:/test_data/")
    }
}
