package com.springboot.listener;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.xpand.starter.canal.annotation.*;

@CanalEventListener
public class CanalDataEventListener {

    /**
     * @description: 增加数据监听
     * @author: xiaomai
     * @date: 2023/3/17 11:30
     */
    @InsertListenPoint
    public void onEventInsert(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        System.out.println("InsertListenPoint");
        rowData.getAfterColumnsList()
                .forEach((c) -> System.out.println("By--Insert: " + c.getName() + " ::   " + c.getValue()));
    }

    /**
     * @description: 修改数据监听
     * @author: xiaomai
     * @date: 2023/3/17 11:30
     */
    @UpdateListenPoint
    public void onEventUpdate(CanalEntry.RowData rowData) {
        System.out.println("UpdateListenPoint");
        rowData.getAfterColumnsList()
                .forEach((c) -> System.out.println("By--Update: " + c.getName() + " ::   " + c.getValue()));
    }

    /**
     * @description: 删除数据监听
     * @author: xiaomai
     * @date: 2023/3/17 11:31
     */
    @DeleteListenPoint
    public void onEventDelete(CanalEntry.RowData rowData) {
        System.out.println("DeleteListenPoint");
        rowData.getBeforeColumnsList()
                .forEach((c) -> System.out.println("By--Delete: " + c.getName() + " ::   " + c.getValue()));
    }

    /**
     * @description: 自定义数据监听
     * @author: xiaomai
     * @date: 2023/3/17 11:31
     */
    @ListenPoint(destination = "example", schema = "canal", table = {"canal_test", "tb_order"},
            eventType = CanalEntry.EventType.UPDATE)
    public void onEventCustomUpdate(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        rowData.getBeforeColumnsList()
                .forEach(c -> System.out.println("Before: " + c.getName() + " ::   " + c.getValue()));
        rowData.getAfterColumnsList()
                .forEach(c -> System.out.println("After: " + c.getName() + " ::   " + c.getValue()));
    }

    @ListenPoint(destination = "example", schema = "zksc_medical", // 所要监听的数据库名
            table = {"queue_up"}, // 所要监听的数据库表名
            eventType = {CanalEntry.EventType.UPDATE, CanalEntry.EventType.INSERT, CanalEntry.EventType.DELETE})
    public void onEventCustomUpdateForTbUser(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        getChangeValue(eventType, rowData);
    }

    public static void getChangeValue(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        if (eventType == CanalEntry.EventType.INSERT){
            rowData.getAfterColumnsList().forEach(column -> {
                // 新增后的数据
                System.out.println(column.getName() + " == " + column.getValue());
            });
        }
        else if (eventType == CanalEntry.EventType.DELETE) {
            rowData.getBeforeColumnsList().forEach(column -> {
                // 获取删除前的数据
                System.out.println(column.getName() + " == " + column.getValue());
            });
        } else {
            rowData.getBeforeColumnsList().forEach(column -> {
                // 打印改变前的字段名和值
                System.out.println(column.getName() + " == " + column.getValue());
            });
            System.out.println("==================================");
            rowData.getAfterColumnsList().forEach(column -> {
                // 打印改变后的字段名和值
                System.out.println(column.getName() + " == " + column.getValue());
            });
        }
    }
}