package day04;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Demo02_SlideWindow {
    public static void main(String[] args) throws Exception {
        // todo 目标 滑动窗口 窗口大小 5s  距离 2s
        // todo 1 环境对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // todo 2 输入
        DataStreamSource<String> source = env.socketTextStream("node1", 9999);
        source.print("源数据：");
        // todo 3 数据处理
        // todo 3.1 将行内容 转化 tuple3
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> map = source.map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(String line) throws Exception {
                String[] arr = line.split(",");
                return Tuple3.of(arr[0], Integer.parseInt(arr[1]), Long.parseLong(arr[2]));
            }
        });
        // todo 3.2 水印 延迟时间
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> watermarks = map.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple3<String, Integer, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner((event, ts) -> event.f2 * 1000L)
        );
        // todo 3.3 keyBy 定义滑动窗口 （5，2） 聚合sum 封装到 tuple2
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = watermarks
                .keyBy(event -> event.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)))
                .sum(1)
                .map(event -> Tuple2.of(event.f0, event.f1)).returns(Types.TUPLE(Types.STRING, Types.INT));
        // todo 4 输出
        res.printToErr();
        // todo 5 执行
        env.execute();

    }
}
