package com.zzw.app;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.zzw.stream.realtime.v1.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.zzw.app.lx
 * @Author zhengwei_zhou
 * @Date 2025/5/12 15:04
 * @description:
 */
public class lx {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        MySqlSource<String> realtimeV1 = FlinkSourceUtil.getMySqlSource("realtime_v1", "*");

        DataStreamSource<String> mySQLSource = env.fromSource(realtimeV1, WatermarkStrategy.noWatermarks(), "MySQL Source");

        mySQLSource.print();




        env.execute();


    }
}
