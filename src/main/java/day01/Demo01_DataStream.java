package day01;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

public class Demo01_DataStream {
    public static void main(String[] args) throws Exception {
        // todo 需求：从socket中读取单词，进行词频统计。
        // todo 1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // todo 设置并行度
        env.setParallelism(1);
        // todo 设置执行模式
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // todo 2.数据源Source
        DataStreamSource<String> source = env.readTextFile("D:\\develop\\java_project\\flinkbase\\data\\wordcount.txt");
//        source.print();
        // todo 3.数据处理Transformation
        // todo 3.1 对单词进行扁平化处理 flatmap
        SingleOutputStreamOperator<String> flatMap = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] wordArr = line.split(",");
                for (String word : wordArr) {
                    out.collect(word);
                }
            }
        });
        flatMap.print();
        // todo 3.2对扁平化处理的数据进行map转换操作,转成(单词,1)
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapData = flatMap.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });
//        mapData.print();
        // todo 3.2对map转换的数据进行keyBy分组操作
        KeyedStream<Tuple2<String, Integer>, String> keyByData = mapData.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        });
        // todo 3.4对分组后的数据进行聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = keyByData.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0, (value1.f1 + value2.f1));
            }
        });
        reduce.print();
        // todo 4.数据输出Sink
        // todo 5.启动流式任务
        env.execute();


    }
}
