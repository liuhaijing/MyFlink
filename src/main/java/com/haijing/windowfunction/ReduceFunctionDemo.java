package com.haijing.windowfunction;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Tuple2;

/**
 * windowfunction
 * 窗口函数之增量聚合函数:reducefunction
 */
public class ReduceFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = source.map(new MapFunction<String, Tuple2<String, Integer>>() {
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
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                        return new Tuple2<>(t1._1, t1._2 + t2._2);
                    }
                });

        reduce.print();


        env.execute("aaaa");
    }
}
