package com.canal.client.entity;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

@Data
public class BinlogDetail {
    private String n;
    private String t;
    private String v;

    @JSONField(name = "origin_val")
    private String originVal;

    @JSONField(name="null")
    private Boolean nullValue;

    private Boolean updated;
}
