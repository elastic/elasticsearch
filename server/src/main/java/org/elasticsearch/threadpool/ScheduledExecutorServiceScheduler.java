/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A {@link Scheduler} which wraps a {@link ScheduledExecutorService}. It always runs the delayed command on the scheduler thread, so the
 * provided {@link Executor} must always be {@link EsExecutors#DIRECT_EXECUTOR_SERVICE}.
 */
public final class ScheduledExecutorServiceScheduler implements Scheduler {
    private final ScheduledExecutorService executor;

    public ScheduledExecutorServiceScheduler(ScheduledExecutorService executor) {
        this.executor = executor;
    }

    @Override
    public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor unused) {
        assert unused == EsExecutors.DIRECT_EXECUTOR_SERVICE : "ScheduledExecutorServiceScheduler never forks, don't even try";
        return Scheduler.wrapAsScheduledCancellable(executor.schedule(command, delay.millis(), TimeUnit.MILLISECONDS));
    }
}
