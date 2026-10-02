/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.capture;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

public class CaptureHandoffTests extends ESTestCase {

    public void testHandsOffToTheExecutor() throws Exception {
        EsThreadPoolExecutor executor = executor(1, 10);
        try {
            CountDownLatch processed = new CountDownLatch(3);
            CaptureHandoff handoff = new CaptureHandoff(executor, captured -> processed.countDown());

            for (int i = 0; i < 3; i++) {
                handoff.accept(captured());
            }

            assertTrue(processed.await(10, TimeUnit.SECONDS));
            assertThat(handoff.handedOff(), equalTo(3L));
            assertThat(handoff.dropped(), equalTo(0L));
        } finally {
            terminate(executor);
        }
    }

    public void testDropsWithoutBlockingWhenTheQueueIsFull() throws Exception {
        EsThreadPoolExecutor executor = executor(1, 2);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch started = new CountDownLatch(1);
        AtomicInteger processed = new AtomicInteger();
        try {
            CaptureHandoff handoff = new CaptureHandoff(executor, captured -> {
                started.countDown();
                safeAwait(release);
                processed.incrementAndGet();
            });

            handoff.accept(captured());
            safeAwait(started); // the single thread is now stuck, the next two fill the queue
            handoff.accept(captured());
            handoff.accept(captured());
            int overflow = between(1, 20);
            for (int i = 0; i < overflow; i++) {
                handoff.accept(captured()); // must return immediately although nothing is being consumed
            }

            assertThat(handoff.handedOff(), equalTo(3L));
            assertThat(handoff.dropped(), equalTo((long) overflow));

            release.countDown();
            assertBusy(() -> assertThat(processed.get(), equalTo(3)));
        } finally {
            release.countDown();
            terminate(executor);
        }
    }

    private EsThreadPoolExecutor executor(int threads, int queueSize) {
        return EsExecutors.newFixed(
            getTestName(),
            threads,
            queueSize,
            EsExecutors.daemonThreadFactory("test", getTestName()),
            new ThreadContext(Settings.EMPTY),
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );
    }

    private static void terminate(EsThreadPoolExecutor executor) {
        executor.shutdownNow();
    }

    private static CapturedSearch captured() {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { 1f }, 10, 100, null, null, List.of(), null);
        return new CapturedSearch(query, List.of(), 1, 1.0);
    }
}
