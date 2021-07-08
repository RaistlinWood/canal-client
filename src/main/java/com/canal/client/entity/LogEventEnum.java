package com.canal.client.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum LogEventEnum {
    
    UPDATE("UPDATE", 'u'),
    INSERT("INSERT", 'i'),
    DELETE("DELETE", 'd');

    private final String code;
    private final Character value;

    public static  Character getValue(String code) {
        if(null == code) {
            return null;
        }

        for(LogEventEnum item : LogEventEnum.values()) {
            if(item.getValue().equals(code)) {
                return item.getValue();
            }
        }
        return null;
    }
}
