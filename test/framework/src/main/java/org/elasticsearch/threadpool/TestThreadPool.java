/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.node.Node;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TestThreadPool extends ThreadPool implements Releasable {

    private final CountDownLatch blockingLatch = new CountDownLatch(1);
    private volatile boolean returnRejectingExecutor = false;
    private volatile ThreadPoolExecutor rejectingExecutor;

    public TestThreadPool(String name, ExecutorBuilder<?>... customBuilders) {
        this(name, Settings.EMPTY, customBuilders);
    }

    public TestThreadPool(String name, Settings settings, ExecutorBuilder<?>... customBuilders) {
        super(Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), name).put(settings).build(), MeterRegistry.NOOP, customBuilders);
    }

    @Override
    public ExecutorService executor(String name) {
        if (returnRejectingExecutor) {
            return rejectingExecutor;
        } else {
            return super.executor(name);
        }
    }

    public void startForcingRejections() {
        if (rejectingExecutor == null) {
            createRejectingExecutor();
        }
        returnRejectingExecutor = true;
    }

    public void stopForcingRejections() {
        returnRejectingExecutor = false;
    }

    @Override
    public void shutdown() {
        blockingLatch.countDown();
        if (rejectingExecutor != null) {
            rejectingExecutor.shutdown();
        }
        super.shutdown();
    }

    @Override
    public void shutdownNow() {
        blockingLatch.countDown();
        if (rejectingExecutor != null) {
            rejectingExecutor.shutdownNow();
        }
        super.shutdownNow();
    }

    private synchronized void createRejectingExecutor() {
        if (rejectingExecutor != null) {
            return;
        }
        ThreadFactory factory = EsExecutors.daemonThreadFactory("reject_thread");
        rejectingExecutor = EsExecutors.newFixed(
            "rejecting",
            1,
            0,
            factory,
            getThreadContext(),
            EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
        );

        CountDownLatch startedLatch = new CountDownLatch(1);
        rejectingExecutor.execute(() -> {
            try {
                startedLatch.countDown();
                blockingLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        try {
            startedLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        ThreadPool.terminate(this, 10, TimeUnit.SECONDS);
    }
}
