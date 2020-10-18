package com.haijing.eventTime;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class EventTimeDemo {
    public static void main(String[] args) throws Exception {
        {


            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStreamSource<String> source = env.socketTextStream("localhost", 8888);

            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            SingleOutputStreamOperator s2 = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor(Time.seconds(10)) {
                @Override
                public long extractTimestamp(Object element) {
                    return Long.parseLong(element.toString().split(",")[0]);
                }
            });
            SingleOutputStreamOperator apply = s2.filter(new FilterFunction<String>() {
                @Override
                public boolean filter(String o) throws Exception {
                    return o.toString().split(",", -1).length >= 2;
                }
            }).keyBy(new KeySelector<String, String>() {
                         @Override
                         public String getKey(String o) throws Exception {
                             return o.split(",", -1)[1];
                         }
                     }
            ).timeWindow(Time.seconds(5))
                    .apply( new WindowFunction<String, String , String, Window>() {
                        @Override
                        public void apply(String o, Window window, Iterable input, Collector out) throws Exception {
                            Iterator iterator = input.iterator();
                            String key = "";
                            int num = 0;
                            while (iterator.hasNext()) {
                                key = iterator.next().toString().split(",", -1)[1];
                                num++;
                            }
                            out.collect(key + ":" + num);
                        }
                    });
            apply.print();

            env.execute("test");
        }

    }
}
