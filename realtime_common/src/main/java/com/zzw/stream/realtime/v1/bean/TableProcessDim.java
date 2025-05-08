package com.zzw.stream.realtime.v1.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.zzw.stream.realtime.v1.bean.TableProcessDim
 * @Author zhengwei_zhou
 * @Date 2025/4/8 8:47
 * @description: TableProcessDim
 */

@AllArgsConstructor
@NoArgsConstructor
@Data
public class TableProcessDim {
    String sourceTable;
    String sinkTable;
    String sinkColumns;
    String sinkFamily;
    String sinkRowKey;
    String op;
}
