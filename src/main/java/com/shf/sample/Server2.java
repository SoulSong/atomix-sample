package com.shf.sample;

import io.atomix.core.Atomix;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Server2 {

    public static void main(String[] args) {
        Atomix atomix = AtomixFactory.createAtomixServer("sample-cluster","member2",
                "127.0.0.1",5680,null,0,
                1,250,1,
                Arrays.asList("member1", "member2", "member3","member4","member5"));
        AtomixFactory.runServer(atomix);
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            log.info("{}", atomix.getCounter("counter").incrementAndGet());
        }, 1, 1, TimeUnit.SECONDS);
    }
}
