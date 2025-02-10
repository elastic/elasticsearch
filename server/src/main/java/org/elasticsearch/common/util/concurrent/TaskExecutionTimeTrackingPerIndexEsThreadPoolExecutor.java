/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

// TODO MP add java doc
public class TaskExecutionTimeTrackingPerIndexEsThreadPoolExecutor extends TaskExecutionTimeTrackingEsThreadPoolExecutor {
    private final ConcurrentHashMap<String, LongAdder> indexExecutionTime;
    private final Map<Runnable, String> runnableToIndexName;
    // TODO MP do we need also EWMA per-index or per-project?

    TaskExecutionTimeTrackingPerIndexEsThreadPoolExecutor(
        String name,
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue,
        Function<Runnable, WrappedRunnable> runnableWrapper,
        ThreadFactory threadFactory,
        RejectedExecutionHandler handler,
        ThreadContext contextHolder,
        EsExecutors.TaskTrackingConfig trackingConfig
    ) {
        super(
            name,
            corePoolSize,
            maximumPoolSize,
            keepAliveTime,
            unit,
            workQueue,
            runnableWrapper,
            threadFactory,
            handler,
            contextHolder,
            trackingConfig
        );
        indexExecutionTime = new ConcurrentHashMap<>();
        runnableToIndexName = new HashMap<>();
    }

    public long getSearchLoadPerIndex(String indexName) {
        // TODO MP check for null maybe we don't need getOrDefault
        return indexExecutionTime.getOrDefault(indexName, new LongAdder()).sum();
    }

    public long getLoadEMWAPerIndex(String indexName) {
        // TODO MP do we need to report load EMWA per index?
        throw new UnsupportedOperationException("Not supported yet");
    }

    public long getSearchLoadPerProject() {
        // TODO MP we probably need to report sl per project?
        throw new UnsupportedOperationException("Not supported yet");
    }

    public long getLoadEMWAPerProject() {
        // TODO MP we probably need to report load EMWA per project?
        throw new UnsupportedOperationException("Not supported yet");
    }

    public void registerIndexNameForRunnable(String indexName, Runnable r) {
        runnableToIndexName.put(r, indexName);
    }

    private void trackExecutionTimePerIndex(Runnable r, long timeSpentExecuting) {
        // TODO MP do we need a LongAdder here?
        String indexName = runnableToIndexName.get(r);
        if (indexName != null) {
            indexExecutionTime.putIfAbsent(indexName, new LongAdder());
            indexExecutionTime.get(indexName).add(timeSpentExecuting);
        }
    }

    @Override
    protected void trackExecutionTime(Runnable r, long taskTime) {
        trackExecutionTimePerIndex(r, taskTime);
        super.trackExecutionTime(r, taskTime);
    }
}
