package com.haijing.WindowAssigner;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;

/**
 * 自定义windowAssigner
 */
public class MyWindowAssigner extends WindowAssigner<Object, TimeWindow> {

    public final  static  Long windowSize = 10*1000L;


    /**
     * 返回元素分配的目标窗口集合
     * @param s
     * @param ts
    * @param content
     * @return
     */
    @Override
    public Collection<TimeWindow> assignWindows(Object s, long ts, WindowAssignerContext content) {

//        注意这里获取时间的方法是从 WindowAssignerContext 中获取 处理时间
        Long startTime = content.getCurrentProcessingTime() -(content.getCurrentProcessingTime()%windowSize);
        Long endTime = startTime + windowSize;
        return Collections.singleton(new TimeWindow(startTime, endTime));
    }

    /**
     * 返回WindowAssigner的默认触发器
     * @param streamExecutionEnvironment
     * @return
     */
    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
        return ProcessingTimeTrigger.create();
    }

    /**
     * 返回WindowAssigner的中窗口的TypeSerializer
     * @param executionConfig
     * @return
     */
    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    /**
     * 是否是基于时间时间的窗口分配器
     * @return
     */
    @Override
    public boolean isEventTime() {
        return false;
    }
}
