package com.canal.client.handle;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "canal")
@Data
public class CanalConfig {
    private String server;
    private String port;
    private List<CanalListen> listenList;
}
