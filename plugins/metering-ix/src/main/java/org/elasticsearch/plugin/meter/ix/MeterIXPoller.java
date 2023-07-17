/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.meter.ix;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.locks.ReentrantLock;

public class MeterIXPoller implements LifecycleComponent {
    private static final Logger logger = LogManager.getLogger(MeterIXPoller.class);
    final ThreadPool threadPool;
    final IndicesService indexServices;
    Lifecycle.State state = Lifecycle.State.INITIALIZED;
    Scheduler.ScheduledCancellable next = null;
    private final ReentrantLock mutex = new ReentrantLock();

    public MeterIXPoller(ThreadPool threadPool, IndicesService indexServices) {
        this.threadPool = threadPool;
        this.indexServices = indexServices;
    }

    @Override
    public void close() {
        mutex.lock();
        try {
            if (state != Lifecycle.State.STOPPED && state != Lifecycle.State.CLOSED) {
                state = Lifecycle.State.CLOSED;
                if (next != null) {
                    next.cancel();
                }
                next = null;
            }
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public Lifecycle.State lifecycleState() {
        return null;
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {

    }

    @Override
    public void start() {
        mutex.lock();
        try {
            if (state == Lifecycle.State.INITIALIZED) {
                next = threadPool.schedule(this::logIndexStats, TimeValue.timeValueSeconds(20), ThreadPool.Names.GENERIC);
                state = Lifecycle.State.STARTED;
            }
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public void stop() {
        mutex.lock();
        try {
            if (state != Lifecycle.State.STOPPED && state != Lifecycle.State.CLOSED) {
                state = Lifecycle.State.STOPPED;
                if (next != null) {
                    next.cancel();
                }
                next = null;
            }
        } finally {
            mutex.unlock();
        }
    }

    public void logIndexStats() {
        logger.warn("[STU] running logIndexStats");
    }
}
