package com.canal.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

import java.net.InetAddress;
import java.net.UnknownHostException;

@SpringBootApplication(scanBasePackages = "com.canal.client")
@Slf4j
public class StartApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(StartApplication.class, args);
        print(context);
    }

    private static void print(ConfigurableApplicationContext context) {
        Environment env = context.getEnvironment();

        String address;
        try {
            address = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            address = "";
        }

        String name = env.getProperty("spring.application.name");
        String protocol = env.getProperty("server.ssl.key-store") != null ? "https" : "http";
        String port = env.getProperty("server.port");
        String profiles = env.getActiveProfiles()[0];

        String builder = "\n--------------------------------------------\n" +
                "Application '{}' is runing! Access URLs:\n\t" +
                "Local:\t\t{}://localhost:{}\n\t" +
                "External:\t{}://{}:{}\n\t" +
                "Profile(s):\t{}" +
                "\n----------------------------------------------------";

        log.info(builder, name, protocol, port, protocol, address, port, profiles);
    }

}
