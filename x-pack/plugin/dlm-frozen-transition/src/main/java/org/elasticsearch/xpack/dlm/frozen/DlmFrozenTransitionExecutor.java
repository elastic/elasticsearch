/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.apache.logging.log4j.LogManager.getLogger;

class DlmFrozenTransitionExecutor implements AutoCloseable {

    private static final Logger logger = getLogger(DlmFrozenTransitionExecutor.class);

    private static final String EXECUTOR_NAME = "dlm-frozen-transition";

    private final Set<String> runningTransitions;
    private final int maxConcurrency;
    private final ExecutorService executor;

    DlmFrozenTransitionExecutor(int maxConcurrency, Settings settings) {
        runningTransitions = ConcurrentHashMap.newKeySet(maxConcurrency);
        this.maxConcurrency = maxConcurrency;
        this.executor = EsExecutors.newFixed(
            EXECUTOR_NAME,
            maxConcurrency,
            0,
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
                task.run();
                logger.info("Transition completed for index [{}]", indexName);
            } catch (Throwable t) {
                logger.error(() -> Strings.format("Error executing transition for index [%s]", indexName), t);
            } finally {
                runningTransitions.remove(indexName);
            }
        };
    }

    @Override
    public void close() throws Exception {
        executor.close();
    }
}
