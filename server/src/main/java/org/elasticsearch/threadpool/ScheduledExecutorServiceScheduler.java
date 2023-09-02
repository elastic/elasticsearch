/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.core.TimeValue;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A {@link Scheduler} which wraps a {@link ScheduledExecutorService}. It ignores the supplied {@code executor} or {@code executorName} and
 * instead uses the inner executor to execute the delayed commands.
 */
public final class ScheduledExecutorServiceScheduler implements Scheduler {
    private final ScheduledExecutorService executor;

    public ScheduledExecutorServiceScheduler(ScheduledExecutorService executor) {
        this.executor = executor;
    }

    private ScheduledCancellable schedule(Runnable command, TimeValue delay) {
        return Scheduler.wrapAsScheduledCancellable(executor.schedule(command, delay.millis(), TimeUnit.MILLISECONDS));
    }

    @Override
    public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor) {
        return schedule(command, delay);
    }

    @SuppressWarnings("removal")
    @Override
    public ScheduledCancellable schedule(Runnable command, TimeValue delay, String executorName) {
        return schedule(command, delay);
    }
}
