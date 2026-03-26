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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
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

    private final ConcurrentHashMap<String, Boolean> submittedTransitions;
    private final ExecutorService executor;
    private final int maxConcurrency;
    private final int maxQueueSize;

    DlmFrozenTransitionExecutor(int maxConcurrency, int maxQueueSize, Settings settings) {
        this.maxConcurrency = maxConcurrency;
        this.maxQueueSize = maxQueueSize;
        this.submittedTransitions = new ConcurrentHashMap<>(maxQueueSize);
        ThreadFactory esThreadFactory = EsExecutors.daemonThreadFactory(settings, EXECUTOR_NAME);
        this.executor = EsExecutors.newFixed(EXECUTOR_NAME, maxConcurrency, maxQueueSize, r -> {
            Thread thread = esThreadFactory.newThread(r);
            if (r instanceof DlmFrozenTransitionRunnable dftr) {
                String name = thread.getName();
                thread.setName(name + "[" + dftr.getIndexName() + "]");
            }
            return thread;
        }, new ThreadContext(settings), EsExecutors.TaskTrackingConfig.DEFAULT);
    }

    public boolean transitionSubmitted(String indexName) {
        return submittedTransitions.containsKey(indexName);
    }

    public boolean hasCapacity() {
        return submittedTransitions.size() < (maxConcurrency + maxQueueSize);
    }

    public List<Runnable> shutdownNow() {
        return executor.shutdownNow();
    }

    public Future<?> submit(DlmFrozenTransitionRunnable task) {
        final String indexName = task.getIndexName();
        submittedTransitions.put(indexName, false);
        try {
            return executor.submit(wrapRunnable(task));
        } catch (Exception e) {
            submittedTransitions.remove(indexName);
            throw e;
        }
    }

    /**
     * Wraps the task with index tracking and error handling. Ensures the index name is always removed from
     * {@link #submittedTransitions} when the thread completes, whether successfully or with an error.
     */
    private Runnable wrapRunnable(DlmFrozenTransitionRunnable task) {
        return () -> {
            final String indexName = task.getIndexName();
            try {
                logger.debug("Starting transition for index [{}]", indexName);
                submittedTransitions.put(indexName, true);
                task.run();
                logger.debug("Transition completed for index [{}]", indexName);
            } catch (Exception ex) {
                logger.error(() -> Strings.format("Error executing transition for index [%s]", indexName), ex);
            } finally {
                submittedTransitions.remove(indexName);
            }
        };
    }

    @Override
    public void close() {
        ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
    }
}
