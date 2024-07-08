/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteTransportException;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.lessThan;

public class FailureCollectorTests extends ESTestCase {

    public void testCollect() throws Exception {
        int maxExceptions = between(1, 100);
        FailureCollector collector = new FailureCollector(maxExceptions);
        List<Exception> cancelledExceptions = List.of(
            new TaskCancelledException("user request"),
            new TaskCancelledException("cross "),
            new TaskCancelledException("on failure")
        );
        List<Exception> nonCancelledExceptions = List.of(
            new IOException("i/o simulated"),
            new IOException("disk broken"),
            new CircuitBreakingException("low memory", CircuitBreaker.Durability.TRANSIENT),
            new CircuitBreakingException("over limit", CircuitBreaker.Durability.TRANSIENT)
        );
        List<Exception> failures = Stream.concat(
            IntStream.range(0, between(1, 500)).mapToObj(n -> randomFrom(cancelledExceptions)),
            IntStream.range(0, between(1, 500)).mapToObj(n -> randomFrom(nonCancelledExceptions))
        ).collect(Collectors.toList());
        Randomness.shuffle(failures);
        Queue<Exception> queue = new ConcurrentLinkedQueue<>(failures);
        Thread[] threads = new Thread[between(1, 4)];
        CyclicBarrier carrier = new CyclicBarrier(threads.length);
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                try {
                    carrier.await(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                Exception ex;
                while ((ex = queue.poll()) != null) {
                    if (randomBoolean()) {
                        collector.unwrapAndCollect(ex);
                    } else {
                        collector.unwrapAndCollect(new RemoteTransportException("disconnect", ex));
                    }
                    if (randomBoolean()) {
                        assertTrue(collector.hasFailure());
                    }
                }
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        assertTrue(collector.hasFailure());
        Exception failure = collector.getFailure();
        assertNotNull(failure);
        assertThat(failure, Matchers.in(nonCancelledExceptions));
        assertThat(failure.getSuppressed().length, lessThan(maxExceptions));
    }

    public void testEmpty() {
        FailureCollector collector = new FailureCollector(5);
        assertFalse(collector.hasFailure());
        assertNull(collector.getFailure());
    }
}
