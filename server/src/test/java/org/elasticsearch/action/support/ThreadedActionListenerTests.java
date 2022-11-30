/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ThreadedActionListenerTests extends ESTestCase {

    public void testRejectionHandling() throws InterruptedException {
        final var listenerCount = between(1, 1000);
        final var countdownLatch = new CountDownLatch(listenerCount);
        final var threadPool = new TestThreadPool(
            "test",
            Settings.EMPTY,
            new FixedExecutorBuilder(Settings.EMPTY, "fixed-bounded-queue", between(1, 3), 10, "fbq", randomBoolean()),
            new FixedExecutorBuilder(Settings.EMPTY, "fixed-unbounded-queue", between(1, 3), -1, "fnq", randomBoolean()),
            new ScalingExecutorBuilder("scaling-drop-if-shutdown", between(1, 3), between(3, 5), TimeValue.timeValueSeconds(1), false),
            new ScalingExecutorBuilder("scaling-reject-if-shutdown", between(1, 3), between(3, 5), TimeValue.timeValueSeconds(1), true)
        );
        final var closeFlag = new AtomicBoolean();
        try {
            final var pools = randomNonEmptySubsetOf(
                List.of("fixed-bounded-queue", "fixed-unbounded-queue", "scaling-drop-if-shutdown", "scaling-reject-if-shutdown")
            );
            final var shutdownUnsafePools = Set.of("fixed-bounded-queue", "scaling-drop-if-shutdown");

            threadPool.generic().execute(() -> {
                for (int i = 0; i < listenerCount; i++) {
                    final var pool = randomFrom(pools);
                    final var listener = new ThreadedActionListener<Void>(
                        logger,
                        threadPool,
                        pool,
                        ActionListener.wrap(countdownLatch::countDown),
                        (pool.equals("fixed-bounded-queue") || pool.startsWith("scaling")) && rarely()
                    );
                    synchronized (closeFlag) {
                        if (closeFlag.get() && shutdownUnsafePools.contains(pool)) {
                            // closing, so tasks submitted to this pool may just be dropped
                            countdownLatch.countDown();
                        } else if (randomBoolean()) {
                            listener.onResponse(null);
                        } else {
                            listener.onFailure(new ElasticsearchException("simulated"));
                        }
                    }
                    Thread.yield();
                }
            });
        } finally {
            synchronized (closeFlag) {
                assertTrue(closeFlag.compareAndSet(false, true));
                threadPool.shutdown();
            }
            assertTrue(threadPool.awaitTermination(10, TimeUnit.SECONDS));
        }
        assertTrue(countdownLatch.await(10, TimeUnit.SECONDS));
    }

}
