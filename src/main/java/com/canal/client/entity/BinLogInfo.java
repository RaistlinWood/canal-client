package com.canal.client.entity;

import lombok.Data;

import java.util.List;

@Data
public class BinLogInfo {

    private String binlog;
    private Long time;
    private Long canalTime;
    private String db;
    private String table;
    private Character event;
    private List<BinlogDetail> columns;
    private List<String> keys;

}
