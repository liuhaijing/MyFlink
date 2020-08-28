package com.haijing.OutPutTag;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class FreeingMonitor extends ProcessFunction<String, String> {

   final OutputTag<String> opt = new OutputTag<String>("freeing-alarms",   Types.STRING);

    @Override
    public void processElement(String s, Context ctx, Collector<String> out) throws Exception {
        if(Integer.parseInt(s)>5){
//            输出副输出数据
            ctx.output(opt, "waring data:"+ s);
        }
//        将全部数据输出
        out.collect(s);
    }
}
