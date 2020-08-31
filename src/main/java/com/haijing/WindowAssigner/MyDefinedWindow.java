package com.haijing.WindowAssigner;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 运行主类
 */
public class MyDefinedWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);

        source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.split(",", -1).length == 2;
            }
        }).keyBy(new KeySelector<String, Object>() {

            @Override
            public Object getKey(String s) throws Exception {
                return s.split(",")[0];
            }
        }).window(new MyWindowAssigner()).reduce(new ReduceFunction<String>() {
            @Override
            public String reduce(String s, String t1) throws Exception {
                String a1 = s.split(",")[0];
                int a2 = Integer.parseInt(s.split(",")[1]);
                int b2 = Integer.parseInt(t1.split(",")[1]);

                return a1 + "," + (a2 + b2);
            }
        }).print();



        env.execute("test");
    }
}
