package com.haijing.OutPutTag;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

/**
 * 副输出功能:大多数DataStream 的算子都只有一个输出，即只能生成一条某个数据类型的结果流。只有split算子可以将一条流拆分成多条类型相同的流。
 * 而处理函数提供的副输出功能准许从同一函数发出多条数据流，且他们的类型可以不同。每个副输出都由一个OutputTag<T> 对象标识，其中T是副输出结果流的类型。
 * 处理函数可以利用Context对象将记录发送至一个或多个副输出。
 */
public class MyOutPutTag {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> read = source.process(new FreeingMonitor());

//       副输出
        DataStream<String> warning = read.getSideOutput(new OutputTag<String>("freeing-alarms", Types.STRING));

        warning.print();
        read.print();
        env.execute("test");

    }
}
