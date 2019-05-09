package com.tjdata.mr.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class MyClassLoader extends ClassLoader {

    private static final Map<String, byte[]> classCache = new HashMap<>(64);

    public static String mainClass = "com.tjdata.mr.core.Algorithm";

    public static String suffix = ".class";

    public static FileSystem fs = null;

    static {
        try {
            fs = FileSystem.get(new Configuration());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        //System.out.println("findClass " + name);
        byte[] byteArray;
        if (classCache.containsKey(name)) {
            byteArray = classCache.get(name);
        } else {
            byteArray = loadByteClass(name);
            classCache.put(name, byteArray);
        }
        if (byteArray == null) {
            throw new ClassNotFoundException();
        }
        return defineClass(name, byteArray, 0, byteArray.length);
    }

    private byte[] loadByteClass(String name) {
//        System.out.println("loadByteClass " + name);
        byte[] byteArray = null;
        try {
            String clazz = name.replaceAll("\\.", "/") + suffix;
            InputStream input = fs.open(new Path(PropertiesReader.getConfig("input_config_root_dir", "E:/test/config/") + "lib/" + clazz));
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int len;
            while ((len = input.read(buffer)) != -1) {
                baos.write(buffer, 0, len);
            }
            byteArray = baos.toByteArray();
            input.close();
            baos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return byteArray;
    }

    public static <T, W> W invoke(String className, T data) {
        try {
            Class clazz = Class.forName(mainClass, true, new MyClassLoader());
            Object instance = clazz.newInstance();
            Method method = clazz.getMethod("invoke", String.class, Object.class);
            return (W) method.invoke(instance, className, data);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        MyClassLoader.invoke("rl", "X01");
        MyClassLoader.invoke("rl", "X01");
    }
}
