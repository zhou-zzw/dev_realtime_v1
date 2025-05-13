package com.zzw.app.utils;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.zzw.stream.realtime.v1.utils.ConfigUtils;

import java.util.Properties;

/**
 * @Package com.stream.common.utils.CdcSourceUtils
 * @Author zhou.han
 * @Date 2024/12/17 11:49
 * @description: MySQL Cdc Source
 */
public class CdcSourceUtils {

    public static MySqlSource<String> getMySQLCdcSource(String database,String table,String username,String pwd,StartupOptions model){
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("database.connectionCharset", "UTF-8");
        debeziumProperties.setProperty("decimal.handling.mode","string");
        debeziumProperties.setProperty("time.precision.mode","connect");
        debeziumProperties.setProperty("database.connectionTimeZone", "Asia/Shanghai");
        return  MySqlSource.<String>builder()
                .hostname(ConfigUtils.getString("mysql.host"))
                .port(ConfigUtils.getInt("mysql.port"))
                .databaseList(database)
                .tableList(table)
                .username(username)
                .password(pwd)
//                .connectionTimeZone(ConfigUtils.getString("mysql.timezone"))
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(model)
                .includeSchemaChanges(true)
                .debeziumProperties(debeziumProperties)
                .build();
    }
}
