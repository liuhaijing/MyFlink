package com.haijing.windowfunction;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * foldFunction
 */
public class FoldFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Map<String, Integer>> fold = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2 map(String s) throws Exception {
                String[] split = s.split(",", -1);
                if (split.length >= 2) {
                    return new Tuple2<String, Integer>(split[0], Integer.parseInt(split[1]));
                }
                return null;
            }
        }).filter(new FilterFunction<Tuple2<String, Integer>>() {
            private Tuple2 o;

            @Override
            public boolean filter(Tuple2 o) throws Exception {
                this.o = o;
                if (o != null) {
                    return true;
                }
                return false;
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2._1;
            }
        }).timeWindow(Time.seconds(10))
                .fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, Map<String, Integer>>() {
                    @Override
                    public Map fold(Map<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
                        accumulator.put(value._1, accumulator.getOrDefault(value._1, 0) + value._2);
                        return accumulator;
                    }
                });

        fold.print();


        env.execute("aaaa");
    }
}
