package com.haijing.windowfunction;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.Iterator;

/**
 * windowfunction
 * 窗口函数之增量聚合函数:Applyfunction
 */
public class ApplyFunctionDemo {
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
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Iterator<Tuple2<String, Integer>> it = input.iterator();
                        String key = "";
                        int num = 0;
                        while (it.hasNext()){

                            Tuple2<String, Integer> next = it.next();

                            if(!"aaa".equals(next._1)){
                                key = next._1;
                                num +=next._2;

                            }

                        }
                        out.collect(new Tuple2<>(key, num));
                    }
                });

        reduce.print();


        env.execute("aaaa");
    }
}
