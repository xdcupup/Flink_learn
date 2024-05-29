package day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Set;

public class Demo07_WordCount_SQL {
    public static void main(String[] args) throws Exception {
        /**
         * @desc: todo 需求：读取socket单词，进行词频统计。 需要使用SQL来实现。
         */
        //todo 1.构建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        //todo 2.构建source表
        /**
         * 源表的schema
         *      |   word    |
         *      |   hello   |
         *      |   hive    |
         *      |   spark   |
         *      |   flink   |
         */
        tEnv.executeSql("create table source ( word string ) with ('connector' = 'socket' ,'hostname' = 'node1' ,'port'= '9999' ,'format' = 'csv' )  ");
        //todo 3.构建sink表
        /**
         * 目标表的schema：
         *      |   word    |   counts   |
         *      |   hello   |      1     |
         *      |   hive    |      1     |
         *      |   spark   |      1     |
         *      |   flink   |      1     |
         */
        tEnv.executeSql("create table sink ( word string,cnt bigint ) with ('connector' = 'print')  ");

        //todo 4.数据处理transformation
        tEnv.executeSql("insert into  sink" + " select word,count(*) as cnt from source group by word  ").await();
        //todo 5.启动流式任务
        env.execute();
    }
}
