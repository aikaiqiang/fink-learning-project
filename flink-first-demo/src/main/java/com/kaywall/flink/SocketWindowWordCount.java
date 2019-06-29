package com.kaywall.flink;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 *
 */
public class SocketWindowWordCount {

    public static void main(String[] args) throws Exception {


        // environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从本地端口号 9000 的 socket 中读取数据的数据源
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");

        // // 解析数据，按 word 分组，开窗，聚合
        DataStream<Tuple2<String, Integer>> windowCounts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : value.split("\\s")) {
                    out.collect(Tuple2.of(word, 1));
                }

            }
        })
        .keyBy(0)
        .timeWindow(Time.seconds(5))
        .sum(1);

        // 打印结果到控制台
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }


}
