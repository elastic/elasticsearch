/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.logging.LogManager.getLogger;

/**
 * DlmFrozenTransitionExecutor is responsible for managing and executing tasks related to
 * frozen transitions in the distributed lifecycle management (DLM) feature.
 * <br>
 * This executor limits the number of concurrent transition tasks based on a configurable capacity
 * and prevents transitions being executed concurrently for the same index.
 * It also ensures that tasks are tracked and cleaned up upon completion or failure.
 */
class DlmFrozenTransitionExecutor implements Closeable {

    private static final Logger logger = getLogger(DlmFrozenTransitionExecutor.class);

    private static final String EXECUTOR_NAME = "dlm-frozen-transition";

    private final Set<String> runningTransitions;
    private final int maxConcurrency;
    private final ExecutorService executor;

    DlmFrozenTransitionExecutor(int maxConcurrency, Settings settings) {
        this.runningTransitions = ConcurrentHashMap.newKeySet(maxConcurrency);
        this.maxConcurrency = maxConcurrency;
        this.executor = EsExecutors.newFixed(
            EXECUTOR_NAME,
            maxConcurrency,
            -1,
            EsExecutors.daemonThreadFactory(settings, EXECUTOR_NAME),
            new ThreadContext(settings),
            EsExecutors.TaskTrackingConfig.DEFAULT
        );
    }

    public boolean isTransitionRunning(String indexName) {
        return runningTransitions.contains(indexName);
    }

    public boolean hasCapacity() {
        return runningTransitions.size() < maxConcurrency;
    }

    public List<Runnable> shutdownNow() {
        return executor.shutdownNow();
    }

    public Future<?> submit(DlmFrozenTransitionRunnable task) {
        final String indexName = task.getIndexName();
        runningTransitions.add(indexName);
        try {
            return executor.submit(wrapRunnable(task));
        } catch (Exception e) {
            runningTransitions.remove(indexName);
            throw e;
        }
    }

    /**
     * Wraps the task with index tracking and error handling. Ensures the index name is always removed from
     * {@link #runningTransitions} when the thread completes, whether successfully or with an error.
     */
    private Runnable wrapRunnable(DlmFrozenTransitionRunnable task) {
        return () -> {
            final String indexName = task.getIndexName();
            try {
                logger.debug("Starting transition for index [{}]", indexName);
                task.run();
                logger.debug("Transition completed for index [{}]", indexName);
            } catch (Exception ex) {
                logger.error(() -> Strings.format("Error executing transition for index [%s]", indexName), ex);
            } finally {
                runningTransitions.remove(indexName);
            }
        };
    }

    @Override
    public void close() {
        ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
    }
}
