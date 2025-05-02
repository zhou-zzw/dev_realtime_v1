package com.zzw.stream.realtime.v1.app.bim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.zzw.stream.realtime.v1.bean.TableProcessDim;
import com.zzw.stream.realtime.v1.constant.Constant;
import com.zzw.stream.realtime.v1.function.HBaseSinkFunction;
import com.zzw.stream.realtime.v1.function.TableProcessFunction;
import com.zzw.stream.realtime.v1.utils.FlinkSourceUtil;
import com.zzw.stream.realtime.v1.utils.HBaseUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Package com.zzw.stream.realtime.v1.app.bim.BaseApp
 * @Author zhengwei_zhou
 * @Date 2025/4/11 9:34
 * @description: BaseApp
 */
public class BaseApp {
    public static void main(String[] args) throws Exception {



                // 获取Flink的流式执行环境，这是构建Flink流式作业的基础
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

                // 设置作业的并行度为4，即每个算子将以4个并行实例运行
                env.setParallelism(4);

                // 启用检查点机制，检查点间隔为5000毫秒（即5秒）
                // 检查点模式为EXACTLY_ONCE，确保每条数据只被处理一次
                env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

                // 使用自定义工具类FlinkSourceUtil获取Kafka数据源
                // 从指定的Kafka主题Constant.TOPIC_DB中消费数据，消费者组为"dim_app"
                KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, "dim_app");


                // 从Kafka数据源创建数据流，不使用水印策略，命名为"kafka_source"
                DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");


                // 此代码行被注释，若取消注释，会将Kafka数据源的数据打印到控制台，用于调试
                // kafkaStrDS.print();

                // 对Kafka数据流进行处理，将JSON字符串转换为JSONObject对象
                // 过滤出符合条件的数据，只保留source字段中db为"realtime_v1"，
                // 操作类型为"c"（创建）、"u"（更新）、"d"（删除）、"r"（读取），
                // 且after字段不为空且长度大于2的数据
                SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                        new ProcessFunction<String, JSONObject>() {
                            @Override
                            public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                                // 将JSON字符串解析为JSONObject对象
                                JSONObject jsonObj = JSON.parseObject(jsonStr);
                                // 获取source字段中的db值
                                String db = jsonObj.getJSONObject("source").getString("db");
                                // 获取操作类型
                                String type = jsonObj.getString("op");
                                // 获取after字段的值
                                String data = jsonObj.getString("after");

                                if ("realtime_v1".equals(db)
                                        && ("c".equals(type)
                                        || "u".equals(type)
                                        || "d".equals(type)
                                        || "r".equals(type))
                                        && data != null
                                        && data.length() > 2
                                ) {
                                    // 符合条件的数据收集到输出流中
                                    out.collect(jsonObj);
                                }
                            }
                        }
                );

                // 此代码行被注释，若取消注释，会将处理后的JSON对象数据流打印到控制台，用于调试
                // jsonObjDS.print();

                // 使用自定义工具类FlinkSourceUtil获取MySQL数据源
                // 从"realtime_v1_config"数据库的"table_process_dim"表中读取数据
                MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("realtime_v1_config", "table_process_dim");

                // 从MySQL数据源创建数据流，不使用水印策略，命名为"mysql_source"，并行度设置为1
                DataStreamSource<String> mysqlStrDS = env
                        .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                        .setParallelism(1);

                // 此代码行被注释，若取消注释，会将MySQL数据源的数据打印到控制台，用于调试
                // mysqlStrDS.print();

                // 将MySQL数据流中的JSON字符串映射为TableProcessDim对象
                // 根据操作类型（op）从before或after字段中获取数据
                SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                        new MapFunction<String, TableProcessDim>() {
                            @Override
                            public TableProcessDim map(String jsonStr) {
                                // 将JSON字符串解析为JSONObject对象
                                JSONObject jsonObj = JSON.parseObject(jsonStr);
                                // 获取操作类型
                                String op = jsonObj.getString("op");
                                TableProcessDim tableProcessDim = null;
                                if ("d".equals(op)) {
                                    // 若操作类型为删除，从before字段获取数据
                                    tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                                } else {
                                    // 其他操作类型，从after字段获取数据
                                    tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                                }
                                // 设置操作类型到TableProcessDim对象中
                                tableProcessDim.setOp(op);
                                return tableProcessDim;
                            }
                        }
                ).setParallelism(1);

                // 此代码行被注释，若取消注释，会将映射后的TableProcessDim数据流打印到控制台，用于调试
                // tpDS.print();

                // 对TableProcessDim数据流进行处理，根据操作类型对HBase表进行创建或删除操作
                tpDS.map(
                        new RichMapFunction<TableProcessDim, TableProcessDim>() {
                            // 定义HBase连接对象
                            private Connection hbaseConn;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                // 在算子初始化时获取HBase连接
                                hbaseConn = HBaseUtil.getHBaseConnection();
                            }

                            @Override
                            public void close() throws Exception {
                                // 在算子关闭时关闭HBase连接
                                HBaseUtil.closeHBaseConnection(hbaseConn);
                            }

                            @Override
                            public TableProcessDim map(TableProcessDim tp) {
                                // 获取操作类型
                                String op = tp.getOp();
                                // 获取目标表名
                                String sinkTable = tp.getSinkTable();
                                // 获取目标列族，按逗号分割
                                String[] sinkFamilies = tp.getSinkFamily().split(",");
                                if ("d".equals(op)) {
                                    // 若操作类型为删除，删除HBase表
                                    HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                                } else if ("r".equals(op) || "c".equals(op)) {
                                    // 若操作类型为读取或创建，创建HBase表
                                    HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                                } else {
                                    // 其他操作类型，先删除再创建HBase表
                                    HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable);
                                    HBaseUtil.createHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                                }
                                return tp;
                            }
                        }
                ).setParallelism(1);

                // 此代码行被注释，若取消注释，会将处理后的TableProcessDim数据流打印到控制台，用于调试
                // tpDS.print();

                // 创建一个MapStateDescriptor，用于广播状态
                MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                        new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor", String.class, TableProcessDim.class);

                // 将TableProcessDim数据流广播出去
                BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

                // 将过滤后的JSON对象数据流与广播流连接起来
                BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

                // 对连接后的数据流进行处理，使用自定义的TableProcessFunction
                SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS
                        .process(new TableProcessFunction(mapStateDescriptor));

                // 此代码行被注释，若取消注释，会将处理后的二元组数据流打印到控制台，用于调试
                // dimDS.print();

                // 将处理后的二元组数据流写入HBase，使用自定义的HBaseSinkFunction
                dimDS.addSink(new HBaseSinkFunction());

                // 启动Flink作业，作业名为"dim"
                env.execute("dim");











//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.setParallelism(4);
//
//        env.enableCheckpointing(5000L , CheckpointingMode.EXACTLY_ONCE);
//
//        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, "dim_app");
//
//        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");
//
//        //kafkaStrDS.print();
//
//        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
//                new ProcessFunction<String, JSONObject>() {
//                    @Override
//                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
//
//                        JSONObject jsonObj = JSON.parseObject(jsonStr);
//                        String db = jsonObj.getJSONObject("source").getString("db");
//                        String type = jsonObj.getString("op");
//                        String data = jsonObj.getString("after");
//
//                        if ("realtime_v1".equals(db)
//                                && ("c".equals(type)
//                                || "u".equals(type)
//                                || "d".equals(type)
//                                || "r".equals(type))
//                                && data != null
//                                && data.length() > 2
//                        ) {
//                            out.collect(jsonObj);
//                        }
//                    }
//                }
//        );
//
////        jsonObjDS.print();
//
//        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("realtime_v1_config", "table_process_dim");
//
//        DataStreamSource<String> mysqlStrDS = env
//                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
//                .setParallelism(1);
//
////        mysqlStrDS.print();
//
//        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
//                new MapFunction<String, TableProcessDim>() {
//                    @Override
//                    public TableProcessDim map(String jsonStr) {
//                        JSONObject jsonObj = JSON.parseObject(jsonStr);
//                        String op = jsonObj.getString("op");
//                        TableProcessDim tableProcessDim = null;
//                        if("d".equals(op)){
//                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
//                        }else{
//                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
//                        }
//                        tableProcessDim.setOp(op);
//                        return tableProcessDim;
//                    }
//                }
//        ).setParallelism(1);
//
////        tpDS.print();
//
//        tpDS.map(
//                new RichMapFunction<TableProcessDim, TableProcessDim>() {
//
//                    private Connection hbaseConn;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        hbaseConn = HBaseUtil.getHBaseConnection();
//                    }
//
//                    @Override
//                    public void close() throws Exception {
//                        HBaseUtil.closeHBaseConnection(hbaseConn);
//                    }
//
//                    @Override
//                    public TableProcessDim map(TableProcessDim tp) {
//                        String op = tp.getOp();
//                        String sinkTable = tp.getSinkTable();
//                        String[] sinkFamilies = tp.getSinkFamily().split(",");
//                        if("d".equals(op)){
//                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable);
//                        }else if("r".equals(op)||"c".equals(op)){
//                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
//                        }else{
//                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
//                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
//                        }
//                        return tp;
//                    }
//                }
//        ).setParallelism(1);
//
////         tpDS.print();
//
//        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
//                new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor",String.class, TableProcessDim.class);
//        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);
//
//        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);
//
//        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS
//                .process(new TableProcessFunction(mapStateDescriptor));
//
////        dimDS.print();
//
//        dimDS.addSink(new HBaseSinkFunction());
//
//        env.execute("dim");

    }
}
