package day02;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Demo05_WordCount_Stream_chain_lambda {
    public static void main(String[] args) throws Exception {
        // todo 需求：从socket中读取单词，进行词频统计。
        // todo 1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // todo 2.数据源Source
        // todo 读取Socket数据源
        // todo socket：hostname+port
        DataStreamSource<String> source = env.socketTextStream("node1", 9999);
//        source.print();
        // todo 3.数据处理Transformation
        // todo 3.1 对单词进行扁平化处理
//        SingleOutputStreamOperator<String> flatMap = ;
//        flatMap.print();
        // todo 3.2对扁平化处理的数据进行map转换操作,转成(单词,1)
//        SingleOutputStreamOperator<Tuple2<String, Integer>> map = ;
//        map.print();
        // todo 3.2对map转换的数据进行keyBy分组操作
//        KeyedStream<Tuple2<String, Integer>, String> keyBy = ;
        // todo 3.4对分组后的数据进行聚合
//        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = keyBy.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> val1, Tuple2<String, Integer> val2) throws Exception {
//                return Tuple2.of(val1.f0, val1.f1 + val2.f1);
//            }
//        });
//        reduce.print();

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = source.
                flatMap(new MySplitter())
                .returns(Types.STRING).
                map(s -> Tuple2.of(s, 1)).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple2 -> tuple2.f0)
                .sum(1);
        sum.print();
        // todo 4.数据输出Sink
        // todo 5.启动流式任务
        env.execute();

    }

    private static class MySplitter implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String line, Collector<String> collector) throws Exception {
            String[] wordArr = line.split(",");
            for (String s : wordArr) {
                collector.collect(s);
            }
        }
    }
}
