package com.zzw.stream.realtime.v1.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * @Package com.zzw.realtime.bean.DimJoinFunction
 * @Author zhengwei_zhou
 * @Date 2025/4/8 8:46
 * @description: DimJoinFunction
 */

public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;
}
