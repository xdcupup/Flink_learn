package day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Demo06_WordCount_TableAPI {
    public static void main(String[] args) throws Exception {
        /**
         * @desc: 需求：从socket中读取单词，进行词频统计。
         * 分析：这里采用Table API来实现。
         * Table是表，也就是说，我们要用代码来操作表。操作包括读取和写入。
         * 所以，我们应该要先准备表。（读取表，写入表）
         * 读取的表：source表
         * 写入的表：sink表
         */
        // todo 1.构建流式执行环境
        //用于构建Table和SQL的顶级对象,流式表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        //2. todo 数据源source表
        DataStreamSource<String> source = env.socketTextStream("node1", 9999);
        //通过tEnv来构建socket数据源
        /**
         * String path：表名称
         * TableDescriptor descriptor：表的描述器，用于构建表
         *
         * Connector:连接器
         * 比如Java/Python读取MySQL，需要一个MySQL的jar包，jar包名字一般是：mysql-connector-java-xxxx.jar
         *这个jar包在代码里叫做驱动包，在Flink中叫做连接器。
         * 我们这里的数据源是哪里？ -> socket
         * 这里的连接器就需要连接socket数据源，这个名字写法：socket，filesystem/kafka
         *
         * 源表的表结构：
         *      |   word    |
         *      |   hello   |
         *      |   flink   |
         *      |   spark   |
         */
        tEnv.createTemporaryTable("source_table",
                TableDescriptor.forConnector("socket")
                        .option("hostname", "node1")
                        .option("port", "9999")
                        .schema(Schema
                                .newBuilder()
                                .column("word", DataTypes.STRING())
                                .build())
                        .format("csv")
                        .build()
        );

        //3.数据输出sink表
        /**
         * sink到标准输出，所以connector的名字为：print
         * print：把数据输出到标准输出或错误输出。
         * sink的表结构：
         *      |   word    |   counts  |
         *      |   hello   |     1     |
         *      |   flink   |     1     |
         *      |   spark   |     1     |
         */
        tEnv.createTemporaryTable("sink_table",
                TableDescriptor
                        .forConnector("print")
                        .schema(
                                Schema
                                        .newBuilder()
                                        .column("word", DataTypes.STRING())
                                        .column("counts", DataTypes.BIGINT())
                                        .build()
                        )
                        .build()
        );

        //4.数据处理 Table API进行处理
        /**
         * insert into sink_table
         * select word,count(*) from source_table group by word
         * from：读取源表的数据
         * groupBy：分组，指定分组的字段
         * select：分组后选择什么字段
         * executeInsert：执行插入操作，把最终的结果插入到其他表中去
         */
        tEnv.from("source_table")
                .groupBy($("word"))
                .select($("word"), $("*").count().as("cnt"))
                .executeInsert("sink_table")
                .await();
        //5.启动流式任务
        env.execute();
    }
}
