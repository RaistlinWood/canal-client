package com.canal.client.handle;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.canal.client.entity.BinLogInfo;
import com.canal.client.entity.BinlogDetail;
import com.canal.client.entity.LogEventEnum;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

@Component
@Slf4j
public class CanalClient implements InitializingBean {

    private final static int BATCH_SIZE = 1000;

    @Resource
    private CanalConfig canalConfig;

    private static final ScheduledThreadPoolExecutor CANAL_THREAD_POOL = new ScheduledThreadPoolExecutor(5,
        new ThreadFactoryBuilder().setNameFormat("canal-client-%d").build(), new ThreadPoolExecutor.AbortPolicy());

    static {
        CANAL_THREAD_POOL.setMaximumPoolSize(5);
    }

    @Override
    public void afterPropertiesSet() {
        for (CanalListen listen : canalConfig.getListenList()) {
            CANAL_THREAD_POOL.execute(() -> this.initConnector(canalConfig.getServer(), canalConfig.getPort(),
                listen.getDestination(), listen.getSubscribe()));
        }
    }

    private void initConnector(String host, String port, String destination, String subscribe) {
        log.info("get binlog start");
        // 创建链接
        CanalConnector connector = CanalConnectors
            .newSingleConnector(new InetSocketAddress(host, Integer.parseInt(port)), destination, "", "");
        try {
            // 打开链接
            connector.connect();
            // 订阅指定数据库表
            connector.subscribe(subscribe);
            // 回滚到未进行ACK的地方，下次FETCH的时候可以从最后一个没有ACK的地方开始拿
            connector.rollback();
            while (true) {
                log.info("{}:try get data start ...", Thread.currentThread().getName());
                // 获取指定数量的数据
                Message message = connector.getWithoutAck(BATCH_SIZE);
                // 获取批量ID
                long batchId = message.getId();
                // 获取批量数量
                int size = message.getEntries().size();
                // 如果没有数据
                if (batchId == -1 || size == 0) {
                    Thread.sleep(5000);
                } else {
                    this.handleData(message.getEntries());
                }
                connector.ack(batchId);
                log.info("{}:try get data end ...", Thread.currentThread().getName());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            connector.disconnect();
        }
    }

    private void handleData(List<CanalEntry.Entry> entries) throws Exception {
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                // 开关事务跳过
                continue;
            }
            CanalEntry.RowChange rowChange;
            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            // 返回值
            BinLogInfo binLogInfo = new BinLogInfo();
            // 获取操作类型
            CanalEntry.EventType eventType = rowChange.getEventType();
            // 组装基础信息
            binLogInfo.setBinlog(entry.getHeader().getLogfileOffset() + "@" + entry.getHeader().getLogfileName());
            binLogInfo.setTime(entry.getHeader().getExecuteTime());
            binLogInfo.setCanalTime(System.currentTimeMillis());
            binLogInfo.setDb(entry.getHeader().getSchemaName());
            binLogInfo.setTable(entry.getHeader().getTableName());
            binLogInfo.setEvent(LogEventEnum.getValue(eventType.name()));
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                if (eventType.equals(CanalEntry.EventType.DELETE)) {
                    // 删除语句
                    binLogInfo.setColumns(getInsertOrDelete(rowData.getBeforeColumnsList()));
                } else if (eventType.equals(CanalEntry.EventType.INSERT)) {
                    // 新增语句
                    binLogInfo.setColumns(getInsertOrDelete(rowData.getAfterColumnsList()));
                } else {
                    // 更新语句
                    binLogInfo.setColumns(getUpdate(rowData.getBeforeColumnsList(), rowData.getAfterColumnsList()));
                }
                String result = JSONObject.toJSONString(binLogInfo);
                log.info("binlog result:{}", result);
                // 消息放入MQ
            }
        }
    }

    /**
     * 处理新增或删除语句
     * 
     * @param columns
     * @return
     */
    private List<BinlogDetail> getInsertOrDelete(List<CanalEntry.Column> columns) {
        return columns.stream().map(column -> {
            BinlogDetail detail = new BinlogDetail();
            detail.setN(column.getName());
            detail.setT(column.getMysqlType());
            detail.setV(column.getValue());
            detail.setNullValue(column.getIsNull());
            detail.setUpdated(true);
            return detail;
        }).collect(Collectors.toList());
    }

    /**
     * 处理修改语句
     * 
     * @param before
     * @param after
     * @return
     */
    private List<BinlogDetail> getUpdate(List<CanalEntry.Column> before, List<CanalEntry.Column> after) {
        List<BinlogDetail> details = new ArrayList<>(before.size());
        for (int i = 0; i < before.size(); i++) {
            BinlogDetail detail = new BinlogDetail();
            detail.setN(before.get(i).getName());
            detail.setT(before.get(i).getMysqlType());
            detail.setV(after.get(i).getValue());
            detail.setOriginVal(before.get(i).getValue());
            detail.setNullValue(after.get(i).getIsNull());
            detail.setUpdated(after.get(i).getUpdated());
            details.add(detail);
        }
        return details;
    }

}
