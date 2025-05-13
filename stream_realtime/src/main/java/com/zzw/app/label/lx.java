package com.zzw.app.label;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.zzw.app.utils.IntervalJoinUserInfoLabelProcessFunc;
import com.zzw.stream.realtime.v1.utils.ConfigUtils;
import com.zzw.stream.realtime.v1.utils.EnvironmentSettingUtils;
import com.zzw.stream.realtime.v1.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @Package com.zzw.app.label.lx
 * @Author zhengwei_zhou
 * @Date 2025/5/13 9:25
 * @description:
 */
public class lx {

    // 从配置文件获取Kafka的bootstrap.servers配置项，用于指定Kafka集群地址
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    // 从配置文件获取Kafka中CDC（Change Data Capture）数据库主题，用于消费数据库变更消息
    private static final String kafka_cdc_db_topic = ConfigUtils.getString("kafka.cdc.db.topic");


    // 程序入口main方法，@SneakyThrows注解用于简化异常处理，自动抛出受检异常
    @SneakyThrows
    public static void main(String[] args) {
        // 设置Hadoop用户名，这里设置为root，可能用于访问Hadoop相关资源
        System.setProperty("HADOOP_USER_NAME","root");
        // 获取Flink流计算执行环境，后续所有流计算操作都基于此环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 应用默认的环境参数配置，可能涉及并行度、检查点等配置
        EnvironmentSettingUtils.defaultParameter(env);


        SingleOutputStreamOperator<String>kafkaCdcDbSource   = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        kafka_botstrap_servers,
                        kafka_cdc_db_topic,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) ->{
                            JSONObject jsonObject = JSONObject.parseObject(event);
                            if(event!=null && jsonObject.containsKey("ts_ms")){
                                try {
                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                }catch (Exception e){
                                    e.printStackTrace();
                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                    return 0L;
                                }
                            }
                            return 0L;
                        }
                        ) ,
                "kafka_cdc_db_source"
        ).uid("kafka_cdc_db_source").name("kafka_cdc_db_source");;


        // 将从Kafka读取的字符串数据转换为JSONObject类型。
        // 使用JSON::parseObject方法进行转换，方便后续对数据进行基于JSON对象结构的操作，
        // 如提取字段、过滤等。转换后的流命名为dataConvertJsonDs。
        SingleOutputStreamOperator<JSONObject> dataConvertJsonDs = kafkaCdcDbSource.map(JSON::parseObject)
                .uid("convert json")
                .name("convert json");


        // 过滤出消息中source.table为user_info的记录。
        // 即从所有Kafka消息中筛选出与用户基本信息相关的记录，
        // 过滤后的流命名为userInfoDs，用于后续对用户基本信息的进一步处理。
        SingleOutputStreamOperator<JSONObject> userInfoDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("user_info"))
                .uid("filter kafka user info")
                .name("filter kafka user info");

        // 对用户基本信息进行处理。
        // 如果消息中的after字段存在且包含birthday字段，将以天数表示的生日转换为ISO格式日期字符串。
        // 这一步是为了将生日数据规范化，方便后续计算年龄、星座等信息。处理后的流命名为finalUserInfoDs。
        SingleOutputStreamOperator<JSONObject> finalUserInfoDs = userInfoDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                JSONObject after = jsonObject.getJSONObject("after");
                if (after != null && after.containsKey("birthday")) {
                    Integer epochDay = after.getInteger("birthday");
                    if (epochDay != null) {
                        LocalDate date = LocalDate.ofEpochDay(epochDay);
                        after.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                    }
                }
                return jsonObject;
            }
        });

        // 过滤出消息中source.table为user_info_sup_msg的记录。
        // 也就是筛选出与用户补充信息相关的记录，过滤后的流命名为userInfoSupDs，
        // 用于后续对用户补充信息的处理。
        SingleOutputStreamOperator<JSONObject> userInfoSupDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("user_info_sup_msg"))
                .uid("filter kafka user info sup")
                .name("filter kafka user info sup");

// 对用户基本信息进行进一步处理。
        // 从after字段中提取关键信息，如用户ID（uid）、用户名（uname）、用户等级（user_level）等。
        // 同时，根据提取的生日信息计算年龄、年代（decade）和星座（zodiac_sign）。
        // 处理后的流命名为mapUserInfoDs。
        SingleOutputStreamOperator<JSONObject> mapUserInfoDs = finalUserInfoDs.map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) {
                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            result.put("uid", after.getString("id"));
                            result.put("uname", after.getString("name"));
                            result.put("user_level", after.getString("user_level"));
                            result.put("login_name", after.getString("login_name"));
                            result.put("phone_num", after.getString("phone_num"));
                            result.put("email", after.getString("email"));
                            result.put("gender", after.getString("gender") != null? after.getString("gender") : "home");
                            result.put("birthday", after.getString("birthday"));
                            result.put("ts_ms", jsonObject.getLongValue("ts_ms"));
                            String birthdayStr = after.getString("birthday");
                            if (birthdayStr != null &&!birthdayStr.isEmpty()) {
                                try {
                                    LocalDate birthday = LocalDate.parse(birthdayStr, DateTimeFormatter.ISO_DATE);
                                    LocalDate currentDate = LocalDate.now(ZoneId.of("Asia/Shanghai"));
                                    int age = calculateAge(birthday, currentDate);
                                    int decade = birthday.getYear() / 10 * 10;
                                    result.put("decade", decade);
                                    result.put("age", age);
                                    String zodiac = getZodiacSign(birthday);
                                    result.put("zodiac_sign", zodiac);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                        return result;
                    }
                }).uid("map userInfo ds")
                .name("map userInfo ds");

        // 对用户补充信息进行处理。
        // 从after字段中提取与用户补充信息相关的字段，如单位高度（unit_height）、创建时间（create_ts）、体重（weight）等。
        // 处理后的流命名为mapUserInfoSupDs。
        SingleOutputStreamOperator<JSONObject> mapUserInfoSupDs = userInfoSupDs.map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) {
                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            result.put("uid", after.getString("uid"));
                            result.put("unit_height", after.getString("unit_height"));
                            result.put("create_ts", after.getLong("create_ts"));
                            result.put("weight", after.getString("weight"));
                            result.put("unit_weight", after.getString("unit_weight"));
                            result.put("height", after.getString("height"));
                            result.put("ts_ms", jsonObject.getLong("ts_ms"));
                        }
                        return result;
                    }
                }).uid("sup userinfo sup")
                .name("sup userinfo sup");


        // 过滤掉uid为空的用户基本信息记录。
        // 确保后续处理的数据中，用户ID（uid）字段不为空，过滤后的流命名为finalUserinfoDs。
        SingleOutputStreamOperator<JSONObject> finalUserinfoDs = mapUserInfoDs.filter(data -> data.containsKey("uid") &&!data.getString("uid").isEmpty());
        // 过滤掉uid为空的用户补充信息记录。
        // 同样确保用户补充信息数据中，用户ID（uid）字段不为空，过滤后的流命名为finalUserinfoSupDs。
        SingleOutputStreamOperator<JSONObject> finalUserinfoSupDs = mapUserInfoSupDs.filter(data -> data.containsKey("uid") &&!data.getString("uid").isEmpty());

        // 根据uid对用户基本信息流进行分组。
        // 将具有相同uid的用户基本信息记录分到同一组，方便后续进行基于分组的操作，
        // 如连接、聚合等，分组后的流命名为keyedStreamUserInfoDs。
        KeyedStream<JSONObject, String> keyedStreamUserInfoDs = finalUserinfoDs.keyBy(data -> data.getString("uid"));
        // 根据uid对用户补充信息流进行分组。
        // 与用户基本信息类似，将相同uid的用户补充信息记录分组，分组后的流命名为keyedStreamUserInfoSupDs。
        KeyedStream<JSONObject, String> keyedStreamUserInfoSupDs = finalUserinfoSupDs.keyBy(data -> data.getString("uid"));

        // 对用户基本信息流和用户补充信息流进行区间连接。
        // 连接的时间区间为[-5分钟, 5分钟]，即两个流中相同uid的记录在时间上相差不超过5分钟时进行连接。
        // 连接后通过自定义的ProcessJoinFunction（IntervalJoinUserInfoLabelProcessFunc）进行处理，
        // 处理后的流命名为processIntervalJoinUserInfo6BaseMessageDs。
        SingleOutputStreamOperator<JSONObject> processIntervalJoinUserInfo6BaseMessageDs = keyedStreamUserInfoDs.intervalJoin(keyedStreamUserInfoSupDs)
                .between(Time.minutes(-5), Time.minutes(5))
                .process(new IntervalJoinUserInfoLabelProcessFunc());

        // 打印连接处理后的结果流。
        // 这一步主要用于调试和查看数据，方便开发人员了解经过一系列处理后的数据内容和格式。
        processIntervalJoinUserInfo6BaseMessageDs.print();

        // 执行Flink作业，作业名称为DbusUserInfo6BaseLabel。
        // 这是启动Flink作业的关键步骤，作业启动后将按照前面定义的逻辑进行数据处理。
        env.execute("DbusUserInfo6BaseLabel");
    }



    // 根据出生日期和当前日期计算年龄。
    // 使用Period类计算两个日期之间的时间间隔，获取其中的年份差值作为年龄。
    private static int calculateAge(LocalDate birthDate, LocalDate currentDate) {
        return Period.between(birthDate, currentDate).getYears();
    }

    // 根据出生日期计算星座。
    // 通过获取出生日期的月份和日期，按照星座日期范围的定义来判断所属星座。
    private static String getZodiacSign(LocalDate birthDate) {
        int month = birthDate.getMonthValue();
        int day = birthDate.getDayOfMonth();

        // 星座日期范围定义
        if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) return "摩羯座";
        else if (month == 1 || month == 2 && day <= 18) return "水瓶座";
        else if (month == 2 || month == 3 && day <= 20) return "双鱼座";
        else if (month == 3 || month == 4 && day <= 19) return "白羊座";
        else if (month == 4 || month == 5 && day <= 20) return "金牛座";
        else if (month == 5 || month == 6 && day <= 21) return "双子座";
        else if (month == 6 || month == 7 && day <= 22) return "巨蟹座";
        else if (month == 7 || month == 8 && day <= 22) return "狮子座";
        else if (month == 8 || month == 9 && day <= 22) return "处女座";
        else if (month == 9 || month == 10 && day <= 23) return "天秤座";
        else if (month == 10 || month == 11 && day <= 22) return "天蝎座";
        else return "射手座";
    }
}