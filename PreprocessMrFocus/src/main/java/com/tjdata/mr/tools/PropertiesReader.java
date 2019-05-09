package com.tjdata.mr.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.InputStream;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;

public class PropertiesReader {

    public static String input_config_root_dir = "";

    private static Hashtable<String, String> config = new Hashtable<>();

    public static FileSystem fs = null;

    public static void init (String dir) {
        try {
            input_config_root_dir = dir;
            fs = FileSystem.get(new Configuration());
        } catch (Exception e) {
            e.printStackTrace();
        }
        load(config);
    }

    private PropertiesReader() {
    }

    public static String getConfig(String name, String default_value) {
        Object o;
        o = config.get(name);
        if (o == null) {
            return default_value;
        } else {
            return "" + o;
        }

    }

    private static void load(Hashtable<String, String> container) {
        String file_name = "/sys.properties";
        try {
            Properties res = new Properties();
            //InputStream inputStream = PropertiesReader.class.getResourceAsStream(file_name);
            InputStream inputStream = fs.open(new Path(input_config_root_dir + file_name));
            res.load(inputStream);
            inputStream.close();
            Enumeration<Object> enu = res.keys();
            String key;
            String value;
            while (enu.hasMoreElements()) {
                key = "" + enu.nextElement();
                value = res.getProperty(key);
                container.put(key, value);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static int getConfig(String name, int default_value) {
        Object o;
        o = config.get(name);
        if (o == null) {
            return default_value;
        } else {
            try {
                return Integer.parseInt("" + o);
            } catch (Exception ex) {
                return default_value;
            }
        }
    }

    public static void main(String[] args) {
        System.out.println(PropertiesReader.getConfig("input_config_root_dir",""));
    }
}

