/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.core.Tuple;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

/**
 * A specialized thread pool executor that tracks the execution time of tasks per index.
 * This executor provides detailed metrics on task execution times per index, which can be useful for performance monitoring and debugging
 */
public class TaskExecutionTimeTrackingPerIndexEsThreadPoolExecutor extends TaskExecutionTimeTrackingEsThreadPoolExecutor {
    private static final Logger logger = LogManager.getLogger(TaskExecutionTimeTrackingPerIndexEsThreadPoolExecutor.class);
    private final ConcurrentHashMap<String, Tuple<LongAdder, ExponentiallyWeightedMovingAverage>> indexExecutionTime;
    private final ConcurrentHashMap<Runnable, String> runnableToIndexName;

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
        runnableToIndexName = new ConcurrentHashMap<>();
    }

    /**
     * Gets the total execution time for tasks associated with a specific index.
     *
     * @param indexName the name of the index
     * @return the total execution time for the index
     */
    public long getSearchLoadPerIndex(String indexName) {
        Tuple<LongAdder, ExponentiallyWeightedMovingAverage> t = indexExecutionTime.get(indexName);
        return (t != null) ? t.v1().sum() : 0;
    }

    /**
     * Gets the exponentially weighted moving average (EWMA) of the execution time for tasks associated with a specific index name.
     *
     * @param indexName the name of the index
     * @return the EWMA of the execution time for the index
     */
    public double getLoadEMWAPerIndex(String indexName) {
        Tuple<LongAdder, ExponentiallyWeightedMovingAverage> t = indexExecutionTime.get(indexName);
        return (t != null) ? t.v2().getAverage() : 0;
    }

    /**
     * Registers an index name for a given runnable task.
     *
     * @param indexName the name of the index
     * @param r the runnable task
     */
    public void registerIndexNameForRunnable(String indexName, Runnable r) {
        runnableToIndexName.put(r, indexName);
    }

    public void stopTrackingIndex(String indexName) {
        try {
            indexExecutionTime.remove(indexName);
        } catch (NullPointerException e) {
            logger.debug("Trying to stop tracking index [{}] that was never tracked", indexName);
        }
    }

    @Override
    protected void trackExecutionTime(Runnable r, long taskTime) {
        try {
            String indexName = runnableToIndexName.get(r);
            if (indexName != null) {
                Tuple<LongAdder, ExponentiallyWeightedMovingAverage> t = indexExecutionTime.computeIfAbsent(
                    indexName,
                    k -> new Tuple<>(new LongAdder(), new ExponentiallyWeightedMovingAverage(trackingConfig.getEwmaAlpha(), 0))
                );
                t.v1().add(taskTime);
                t.v2().addValue(taskTime);
                logger.info("Task execution time for index [{}] is [{}] [{}]", indexName, taskTime, Thread.currentThread().getName());
            }
            super.trackExecutionTime(r, taskTime);
        } finally {
            runnableToIndexName.remove(r);
        }
    }
}
