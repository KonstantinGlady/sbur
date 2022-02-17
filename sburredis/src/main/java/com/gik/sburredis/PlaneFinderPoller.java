package com.gik.sburredis;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

@EnableScheduling
@Component
public class PlaneFinderPoller {

    private WebClient webClient = WebClient.create("http://localhost:7634/aircraft");

    private final RedisConnectionFactory factory;
    private final RedisOperations<String, Aircraft> operations;

    public PlaneFinderPoller(RedisConnectionFactory factory, RedisOperations<String, Aircraft> operations) {
        this.factory = factory;
        this.operations = operations;
    }

    @Scheduled(fixedRate = 1000)
    private void pollPlanes() {

        factory.getConnection().serverCommands().flushDb();

        webClient.get()
                .retrieve()
                .bodyToFlux(Aircraft.class)
                .filter(plane -> !plane.getReg().isEmpty())
                .toStream()
                .forEach(ac -> operations.opsForValue().set(ac.getReg(), ac));

        operations
                .opsForValue()
                .getOperations()
                .keys("*")
                .forEach(ac -> System.out.println(operations.opsForValue().get(ac)));
    }
}
