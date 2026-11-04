/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Runs the idle timeouts of result streams. A blocked read of a Flight stream cannot be given a deadline of its own, so a timer
 * cancels the stream when nothing has arrived in time. One thread serves every stream of the node and it ends when nothing is
 * scheduled.
 */
final class FlightWatchdog {

    private static final class Holder {
        static final ScheduledThreadPoolExecutor SCHEDULER = create();

        private static ScheduledThreadPoolExecutor create() {
            ScheduledThreadPoolExecutor scheduler = new Scheduler.SafeScheduledThreadPoolExecutor(
                1,
                EsExecutors.daemonThreadFactory(Settings.EMPTY, "esql-flight-watchdog")
            );
            scheduler.setRemoveOnCancelPolicy(true);
            scheduler.setKeepAliveTime(1, TimeUnit.SECONDS);
            scheduler.allowCoreThreadTimeOut(true);
            return scheduler;
        }
    }

    private FlightWatchdog() {}

    static ScheduledFuture<?> schedule(Runnable task, TimeValue after) {
        return Holder.SCHEDULER.schedule(task, after.nanos(), TimeUnit.NANOSECONDS);
    }
}
