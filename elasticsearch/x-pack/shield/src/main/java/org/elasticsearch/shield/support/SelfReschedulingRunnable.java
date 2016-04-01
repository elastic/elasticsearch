/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ScheduledFuture;

public class SelfReschedulingRunnable extends AbstractRunnable {

    private final AbstractRunnable runnable;
    private final ThreadPool threadPool;
    private final TimeValue interval;
    private final String executorName;
    private final ESLogger logger;

    private ScheduledFuture<?> scheduledFuture = null;
    private volatile boolean run = false;

    public SelfReschedulingRunnable(AbstractRunnable runnable, ThreadPool threadPool, TimeValue interval, String executorName,
                                    ESLogger logger) {
        this.runnable = runnable;
        this.threadPool = threadPool;
        this.interval = interval;
        this.executorName = executorName;
        this.logger = logger;
    }

    public synchronized void start() {
        if (run != false || scheduledFuture != null) {
            throw new IllegalStateException("start should not be called again before calling stop");
        }
        run = true;
        scheduledFuture = threadPool.schedule(interval, executorName, this);
    }

    @Override
    public synchronized void onAfter() {
        if (run) {
            scheduledFuture = threadPool.schedule(interval, executorName, this);
        }
    }

    @Override
    public void onFailure(Throwable t) {
        logger.warn("failed to run scheduled task", t);
    }

    @Override
    protected void doRun() throws Exception {
        if (run) {
            runnable.run();
        }
    }

    public synchronized void stop() {
        if (run == false) {
            throw new IllegalStateException("stop called but not started or stop called twice");
        }
        run = false;
        FutureUtils.cancel(scheduledFuture);
        scheduledFuture = null;
    }

    // package private for testing
    ScheduledFuture<?> getScheduledFuture() {
        return scheduledFuture;
    }
}
