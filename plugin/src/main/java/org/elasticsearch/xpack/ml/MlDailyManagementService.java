/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Supplier;

/**
 * A service that runs once a day and triggers management tasks.
 */
public class MlDailyManagementService implements Releasable {

    private static final Logger LOGGER = Loggers.getLogger(MlDailyManagementService.class);

    private final ThreadPool threadPool;

    /**
     * An interface to abstract the calculation of the delay to the next execution.
     * Needed to enable testing.
     */
    private final Supplier<TimeValue> schedulerProvider;

    private final List<Listener> listeners;
    private volatile ScheduledFuture<?> future;

    MlDailyManagementService(ThreadPool threadPool, List<Listener> listeners, Supplier<TimeValue> scheduleProvider) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.listeners = new ArrayList<>(listeners);
        this.schedulerProvider = Objects.requireNonNull(scheduleProvider);
    }

    public MlDailyManagementService(ThreadPool threadPool, List<Listener> listeners) {
        this(threadPool, listeners, createAfterMidnightScheduleProvider());
    }

    private static Supplier<TimeValue> createAfterMidnightScheduleProvider() {
        return () -> {
            DateTime now = DateTime.now(ISOChronology.getInstance());
            DateTime next = now.plusDays(1).withTimeAtStartOfDay().plusMinutes(30);
            return TimeValue.timeValueMillis(next.getMillis() - now.getMillis());
        };
    }

    public void start() {
        LOGGER.debug("Starting ML daily management service");
        scheduleNext();
    }

    public void stop() {
        LOGGER.debug("Stopping ML daily management service");
        if (future != null && future.isCancelled() == false) {
            FutureUtils.cancel(future);
        }
    }

    public boolean isStarted() {
        return future != null;
    }

    @Override
    public void close() {
        stop();
    }

    private void scheduleNext() {
        try {
            future = threadPool.schedule(schedulerProvider.get(), ThreadPool.Names.GENERIC, () -> triggerListeners());
        } catch (EsRejectedExecutionException e) {
            if (e.isExecutorShutdown()) {
                LOGGER.debug("failed to schedule next management task; shutting down", e);
            } else {
                throw e;
            }
        }
    }

    private void triggerListeners() {
        LOGGER.info("triggering scheduled [ML] management tasks");
        for (Listener listener : listeners) {
            listener.onTrigger();
        }
        scheduleNext();
    }

    public interface Listener {
        void onTrigger();
    }
}
