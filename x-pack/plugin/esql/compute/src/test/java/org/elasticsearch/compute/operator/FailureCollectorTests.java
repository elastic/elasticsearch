/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportException;
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

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
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
        assertTrue(
            "cancellation exceptions must be ignored",
            ExceptionsHelper.unwrapCausesAndSuppressed(failure, t -> t instanceof TaskCancelledException).isEmpty()
        );
        assertTrue(
            "remote transport exception must be unwrapped",
            ExceptionsHelper.unwrapCausesAndSuppressed(failure, t -> t instanceof TransportException).isEmpty()
        );
    }

    public void testEmpty() {
        FailureCollector collector = new FailureCollector(5);
        assertFalse(collector.hasFailure());
        assertNull(collector.getFailure());
    }

    public void testTransportExceptions() {
        FailureCollector collector = new FailureCollector(5);
        collector.unwrapAndCollect(new NodeDisconnectedException(DiscoveryNodeUtils.builder("node-1").build(), "/field_caps"));
        collector.unwrapAndCollect(new TransportException(new IOException("disk issue")));
        Exception failure = collector.getFailure();
        assertNotNull(failure);
        assertThat(failure, instanceOf(NodeDisconnectedException.class));
        assertThat(failure.getMessage(), equalTo("[][0.0.0.0:1][/field_caps] disconnected"));
        Throwable[] suppressed = failure.getSuppressed();
        assertThat(suppressed, arrayWithSize(1));
        assertThat(suppressed[0], instanceOf(IOException.class));
    }

    public void testErrorCategory() {
        FailureCollector collector = new FailureCollector(5);
        collector.unwrapAndCollect(new NoShardAvailableActionException(new ShardId("test", "n/a", 1), "not ready"));
        collector.unwrapAndCollect(
            new TransportException(new CircuitBreakingException("request is too large", CircuitBreaker.Durability.TRANSIENT))
        );
        Exception failure = collector.getFailure();
        assertNotNull(failure);
        assertThat(failure, instanceOf(CircuitBreakingException.class));
        assertThat(failure.getMessage(), equalTo("request is too large"));
        assertThat(failure.getSuppressed(), arrayWithSize(1));
        assertThat(failure.getSuppressed()[0], instanceOf(NoShardAvailableActionException.class));
        assertThat(failure.getSuppressed()[0].getMessage(), equalTo("not ready"));
    }
}
