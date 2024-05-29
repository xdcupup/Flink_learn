package day04;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.reflect.internal.Trees;

public class Day04_TumbleWindow {
    public static void main(String[] args) throws Exception {
        // todo :滚动窗口
        // todo 1 创建环境对象
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // todo 2 输入
        DataStreamSource<String> source = env.socketTextStream("node1", 9999);
        source.print();
        // todo 3 数据处理
        // todo 3.1 将行内容转换为元组
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> map = source.map(new MapFunction<String, Tuple3<String, Integer, Long>>() {
            @Override
            public Tuple3<String, Integer, Long> map(String line) throws Exception {
                String[] arr = line.split(",");
                return Tuple3.of(arr[0], Integer.parseInt(arr[1]), Long.parseLong(arr[2]));
            }
        });
        // todo 3.2 设置延迟时间
        SingleOutputStreamOperator<Tuple3<String, Integer, Long>> watermarks = map.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple3<String, Integer, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<String, Integer, Long> t3, long ts) {
                                return t3.f2 * 1000L;
                            }
                        })
        );
        // todo 3.3 分组滚动聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = watermarks.keyBy(event -> event.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .map(new MapFunction<Tuple3<String, Integer, Long>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple3<String, Integer, Long> t3) throws Exception {
                        return Tuple2.of(t3.f0, t3.f1);
                    }
                });
        // todo 4 输出
        res.print();
        // todo 5 执行

        env.execute();
    }
}
