package com.kuaishou.kcode;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;

/**
 * @author kcode
 * Created on 2020-05-20
 */
public class KcodeMain {

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        String dataDir = "C:/Users/wjj/Desktop/data/";
        String warmup_test_data_path = dataDir + "warmup-test.data";
        String result_data_path = dataDir + "result-test.data";
        // "demo.data" 是你从网盘上下载的测试数据，这里直接填你的本地绝对路径
        InputStream fileInputStream = new FileInputStream(warmup_test_data_path);
        Class<?> clazz = Class.forName("com.kuaishou.kcode.KcodeQuestion");
        Object instance = clazz.newInstance();
        Method prepareMethod = clazz.getMethod("prepare", InputStream.class);
        Method getResultMethod = clazz.getMethod("getResult", Long.class, String.class);
        // 调用prepare()方法准备数据
        prepareMethod.invoke(instance, fileInputStream);

        // 验证正确性
        // "result.data" 是你从网盘上下载的结果数据，这里直接填你的本地绝对路径
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(result_data_path)));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] split = line.split("\\|");
            String[] keys = split[0].split(",");
            // 调用getResult()方法
            Object result = getResultMethod.invoke(instance, new Long(keys[0]), keys[1]);
            if (!split[1].equals(result)) {
                System.out.print("fail: ");
                System.out.println(result);
                System.out.println(split[1]);
                return;
            }
        }
        System.out.println("success");
        long endTime = System.currentTimeMillis();
        System.out.println("程序运行时间：" + (endTime - startTime) + "ms");
    }
}
