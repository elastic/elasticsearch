/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.history;

import org.elasticsearch.alerts.AlertsPlugin;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.BlockingQueue;

/**
 *
 */
public class InternalAlertsExecutor implements AlertsExecutor {

    private final ThreadPool threadPool;

    @Inject
    public InternalAlertsExecutor(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public BlockingQueue queue() {
        return executor().getQueue();
    }

    @Override
    public long largestPoolSize() {
        return executor().getLargestPoolSize();
    }

    @Override
    public void execute(Runnable runnable) {
        executor().execute(runnable);
    }

    private EsThreadPoolExecutor executor() {
        return (EsThreadPoolExecutor) threadPool.executor(AlertsPlugin.NAME);
    }
}