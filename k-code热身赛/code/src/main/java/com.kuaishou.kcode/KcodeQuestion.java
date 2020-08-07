package com.kuaishou.kcode;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kcode
 * Created on 2020-05-20
 */
public class KcodeQuestion {
    // 初始化容量与负载因子
    // 用来存储读入的数据，以方法名为 key ，value值为调用方法的时间/1000,将 方法耗时 add 进入 node 节点中进行过计算
    private Map<String, Map<Long, Node>> record = new HashMap<>(256, 0.75f);

    /**
     * prepare() 方法用来接受输入数据集，数据集格式参考README.md
     *
     * @param inputStream
     */
    public void prepare(InputStream inputStream) throws IOException, InterruptedException {
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        final int BUFFER_SIZE = 512 * 1024 * 1024;
        char[] storeBuffer = new char[BUFFER_SIZE];
        char[] dealingBuffer = new char[BUFFER_SIZE];
        // 开一个线程将数据读入到 storebuffer中外加简单处理一下数据
        Thread thread = new Thread(new ReadInputToBuffer(inputStreamReader, storeBuffer));
        thread.start();
        thread.join();

        StringBuilder lastRest = new StringBuilder();
        boolean reachInputEnd = false;
        while (!reachInputEnd) {
            //swap
            char[] tmp = storeBuffer;
            storeBuffer = dealingBuffer;
            dealingBuffer = tmp;

            thread = new Thread(new ReadInputToBuffer(inputStreamReader, storeBuffer));
            thread.start();

            // find the last '\n'
            int validEnd = dealingBuffer.length - 1;
            // 用来存储最后一个 '\n'与 dealingBuffer.length - 1之间的数据
            StringBuilder restSb = new StringBuilder();
            if (dealingBuffer[dealingBuffer.length - 1] != 0) {
                if (dealingBuffer[0] == 0)
                    return;
                while (dealingBuffer[validEnd] != '\n') {
                    restSb.append(dealingBuffer[validEnd]);
                    validEnd--;
                }
                restSb.reverse();
            } else {
                reachInputEnd = true;
                while (validEnd >= 0 && dealingBuffer[validEnd] == 0) {
                    validEnd--;
                }
            }

            int idx = 0;
            // 对第一次读入可能出现的最后一部分不完整数据与第二次读入的首部分数据做一个拼接，完成数据的读入
            if (lastRest.length() != 0) {
                // 对 dealingbuffer 第一个 ’/n‘ 之前的数据 append做一个拼接
                while (dealingBuffer[idx] != '\n') {
                    lastRest.append(dealingBuffer[idx]);
                    idx++;
                }
                idx++;

                char[] arr = new char[lastRest.length()];
                for (int i = 0; i < arr.length; i++) {
                    arr[i] = lastRest.charAt(i);
                }
                int arrIdx = 0;
                long timeStamp = 0;
                while (arr[arrIdx] != ',') {
                    timeStamp = timeStamp * 10 + arr[arrIdx] - '0';
                    arrIdx++;
                }
                timeStamp = timeStamp / 1000;
                arrIdx++;
                int start = arrIdx, count = 0;
                while (arr[arrIdx] != ',') {
                    count++;
                    arrIdx++;
                }
                String methodName = String.valueOf(arr, start, count);
                arrIdx++;
                int msCost = 0;
                while (arrIdx < arr.length) {
                    msCost = msCost * 10 + arr[arrIdx] - '0';
                    arrIdx++;
                }

                //加入哈希表
                if (!record.containsKey(methodName))
                    record.put(methodName, new HashMap<>(256, 0.75f));
                Map<Long, Node> methodMap = record.get(methodName);
                if (!methodMap.containsKey(timeStamp))
                    methodMap.put(timeStamp, new Node());
                methodMap.get(timeStamp).add(msCost);
            }

            //process remain lines
            while (idx < validEnd) {
                long timeStamp = 0;
                while (dealingBuffer[idx] != ',') {
                    timeStamp = timeStamp * 10 + dealingBuffer[idx] - '0';
                    idx++;
                }
                timeStamp = timeStamp / 1000;
                idx++;
                int start = idx, count = 0;
                while (dealingBuffer[idx] != ',') {
                    count++;
                    idx++;
                }
                String methodName = String.valueOf(dealingBuffer, start, count);
                idx++;
                int msCost = 0;
                while (dealingBuffer[idx] != '\n') {
                    msCost = msCost * 10 + dealingBuffer[idx] - '0';
                    idx++;
                }
                idx++;

                //加入哈希表,以 getUserName为 key ，1589761895，103为 value
                if (!record.containsKey(methodName))
                    record.put(methodName, new HashMap<>(256, 0.75f));
                Map<Long, Node> methodMap = record.get(methodName);
                if (!methodMap.containsKey(timeStamp))
                    methodMap.put(timeStamp, new Node());
                methodMap.get(timeStamp).add(msCost);
            }

            lastRest = restSb;
            thread.join();
        }
    }

    /**
     * getResult() 方法是由kcode评测系统调用，是评测程序正确性的一部分，请按照题目要求返回正确数据
     * 输入格式和输出格式参考 README.md
     * \
     *
     * @param timestamp  秒级时间戳
     * @param methodName 方法名称
     */
    public String getResult(Long timestamp, String methodName) throws InterruptedException {
        return record.get(methodName).get(timestamp).getCriterion();
    }
}

// 开启一个线程用于缓存数据，将 inputstreamread 的数据读入到 缓冲字符数组 buffer 中
// 对 缓冲字符数组做一个处理，若满了，第一个与最后一个都置为 0 ，若没满则为最后一组数据，用0补足
class ReadInputToBuffer implements Runnable {
    // 字符流用于存储数据
    private InputStreamReader inputStreamReader;
    private char[] buffer;

    public ReadInputToBuffer(InputStreamReader inputStreamReader, char[] buffer) {
        this.inputStreamReader = inputStreamReader;
        this.buffer = buffer;
    }

    @Override
    public void run() {
        try {
            //返回读取的字符数的大小
            // 将 inputstreamreader 数据读入到 buffer 中
            int numChar = inputStreamReader.read(buffer);
            if (numChar == -1) {
                buffer[buffer.length - 1] = 0;
                buffer[0] = 0;
            } else if (numChar < buffer.length) {
                // 最后数据不足的地方用 null 补足 char a = 0 ,转化为 ascii 码为 null
                for (int i = numChar; i < buffer.length; i++) {
                    buffer[i] = 0;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

// 对结果进行一个处理
class MyIntArray {
    private final int DEFAULT_SIZE = 16;
    // 用来存储 方法耗时的值
    private int[] arr = new int[DEFAULT_SIZE];
    // 记录方法耗时的个数
    private int N = 0;
    private long sum = 0;
    private int max = Integer.MIN_VALUE;
    private int min = Integer.MAX_VALUE;

    private int p99 = -1;
    private int p50 = -1;


    public long sum() {
        return sum;
    }

    public int max() {
        return max;
    }

    public boolean isEmpty() {
        return N == 0;
    }

    public int size() {
        return N;
    }


    private void calculateP50P99() {
        //使用计数排序的思想
        int[] countArr = new int[max - min + 1];
        for (int i = 0; i < N; i++) {
            countArr[arr[i] - min]++;
        }
        int p99k = N - (int) Math.ceil(N * 0.99) + 1;
        int p50k = N - (int) Math.ceil(N * 0.50) + 1;
        for (int i = countArr.length - 1; i >= 0; i--) {
            if (p99 == -1 && countArr[i] >= p99k) {
                p99 = i + min;
            }
            if (countArr[i] >= p50k) {
                p50 = i + min;
                return;
            }
            p99k -= countArr[i];
            p50k -= countArr[i];
        }
    }

    // 2倍数组扩容
    private void resize(int size) {
        int[] tmp = new int[size];
        System.arraycopy(arr, 0, tmp, 0, N);
        arr = tmp;
    }

    // 计算 sum，max，min值
    public void add(int item) {
        // 如果达到数组 arr 最大值的最大值，则 2 倍扩容
        if (N == arr.length) resize(arr.length * 2);
        arr[N++] = item;
        sum += item;
        if (item > max)
            max = item;
        if (item < min)
            min = item;
    }
    // P99, P50,分别对应99分位响应时间，中位数时间，
//    计算第p百分位数。
//            - 第1步：以递增顺序排列原始数据（即从小到大排列）。
//            - 第2步：计算指数i=n*p%
//            - 第3步：
//            - 1）若 i 不是整数，将 i 向上取整，对应位置即为第p百分位数的位置。
//            - 2) 若i是整数，则第p百分位数是第i项的值。

    public int getP99() {
        if (p99 == -1)
            calculateP50P99();
        return p99;
    }

    public int getP50() {
        if (p50 == -1)
            calculateP50P99();
        return p50;
    }
}

class Node {
    MyIntArray array;
    String criterionCache;

    public Node() {
        this.array = new MyIntArray();
    }

    public void add(int item) {
        array.add(item);
    }

    public String getCriterion() {
        if (criterionCache == null) {
            long sum = array.sum();
            int size = array.size();
            int qps = size;
            int p99 = array.getP99();
            int p50 = array.getP50();
            int avg = (int) Math.ceil(((double) sum) / size);
            int max = array.max();
            criterionCache = qps + "," + p99 + "," + p50 + "," + avg + "," + max;
        }
        return criterionCache;
    }
}
