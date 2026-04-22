/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.dlm.DataStreamLifecycleErrorStore;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.logging.LogManager.getLogger;

/**
 * DLMFrozenTransitionExecutor is responsible for managing and executing tasks related to
 * frozen transitions in the distributed lifecycle management (DLM) feature.
 * <br>
 * This executor limits the number of concurrent transition tasks based on a configurable capacity
 * and prevents transitions being executed concurrently for the same index.
 * It also ensures that tasks are tracked and cleaned up upon completion or failure.
 */
class DLMFrozenTransitionExecutor implements Closeable {

    private static final Logger logger = getLogger(DLMFrozenTransitionExecutor.class);
    private static final String EXECUTOR_NAME = "dlm-frozen-transition";

    private final Map<String, Boolean> submittedTransitions;
    private final ExecutorService executor;
    private final int maxConcurrency;
    private final int maxQueueSize;
    private final ClusterService clusterService;
    private final DataStreamLifecycleErrorStore errorStore;
    private final MasterServiceTaskQueue<UnmarkIndexForFrozenTask> unmarkIndexForDlmFrozenConversionQueue;
    private final DLMFrozenTransitionSettings frozenTransitionSettings;

    DLMFrozenTransitionExecutor(
        ClusterService clusterService,
        int maxConcurrency,
        int maxQueueSize,
        Settings settings,
        DLMFrozenTransitionSettings frozenTransitionSettings,
        DataStreamLifecycleErrorStore errorStore
    ) {
        this.maxConcurrency = maxConcurrency;
        this.maxQueueSize = maxQueueSize;
        this.submittedTransitions = new ConcurrentHashMap<>(maxQueueSize);
        ThreadFactory esThreadFactory = EsExecutors.daemonThreadFactory(settings, EXECUTOR_NAME);
        this.executor = EsExecutors.newFixed(EXECUTOR_NAME, maxConcurrency, maxQueueSize, r -> {
            Thread thread = esThreadFactory.newThread(r);
            if (r instanceof WrappedDlmFrozenTransitionRunnable runnable) {
                String name = thread.getName();
                thread.setName(name + "[" + runnable.getIndexName() + "]");
            }
            return thread;
        }, new ThreadContext(settings), EsExecutors.TaskTrackingConfig.DEFAULT);
        this.clusterService = clusterService;
        this.frozenTransitionSettings = frozenTransitionSettings;
        this.errorStore = errorStore;
        this.unmarkIndexForDlmFrozenConversionQueue = clusterService.createTaskQueue(
            "dlm-unmark-index-for-frozen",
            Priority.LOW,
            new UnmarkIndexForDLMFrozenExecutor()
        );
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

    public Future<?> submit(DLMFrozenTransitionRunnable task) {
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
    private Runnable wrapRunnable(DLMFrozenTransitionRunnable task) {
        return new WrappedDlmFrozenTransitionRunnable(task);
    }

    @Override
    public void close() {
        ThreadPool.terminate(executor, 10, TimeUnit.SECONDS);
    }

    public static class UnmarkIndexForDLMFrozenExecutor implements ClusterStateTaskExecutor<UnmarkIndexForFrozenTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<UnmarkIndexForFrozenTask> batchExecutionContext) {
            var state = batchExecutionContext.initialState();
            for (final var taskContext : batchExecutionContext.taskContexts()) {
                try {
                    final UnmarkIndexForFrozenTask task = taskContext.getTask();
                    state = task.execute(state);
                    taskContext.success(task);
                } catch (Exception e) {
                    taskContext.onFailure(e);
                }
            }
            return state;
        }
    }

    private class WrappedDlmFrozenTransitionRunnable implements Runnable {
        private final DLMFrozenTransitionRunnable task;

        private WrappedDlmFrozenTransitionRunnable(DLMFrozenTransitionRunnable task) {
            this.task = task;
        }

        @Override
        public void run() {
            final String indexName = task.getIndexName();
            try {
                logger.debug("Starting transition for index [{}]", indexName);
                Boolean previousValue = submittedTransitions.put(indexName, true);
                assert Boolean.FALSE.equals(previousValue)
                    : "expected the previous value to exist and be false, but it was " + previousValue;
                task.run();
                logger.debug("Transition completed for index [{}]", indexName);
            } catch (DLMUnrecoverableException err) {
                logger.debug(
                    "DLM encountered an unrecoverable error while converting [{}] "
                        + "to a frozen index, submitting task to unmark it for conversion",
                    indexName
                );
                unmarkIndexForDlmFrozenConversionQueue.submitTask(
                    "dlm-unmark-frozen-" + indexName,
                    new UnmarkIndexForFrozenTask(
                        task.getProjectId(),
                        task.getIndexName(),
                        ActionListener.wrap(
                            resp -> logger.debug("DLM successfully unmarked index [{}] for frozen conversion", indexName),
                            exception -> {
                                errorStore.recordAndLogError(
                                    task.getProjectId(),
                                    indexName,
                                    exception,
                                    Strings.format("Error unmarking index [%s] for conversion to frozen index", indexName),
                                    frozenTransitionSettings.getErrorRetryInterval()
                                );
                            }
                        )
                    ),
                    null
                );
            } catch (Exception ex) {
                errorStore.recordAndLogError(
                    task.getProjectId(),
                    indexName,
                    ex,
                    Strings.format("Error executing transition for index [%s]", indexName),
                    frozenTransitionSettings.getErrorRetryInterval()
                );
            } finally {
                submittedTransitions.remove(indexName);
            }
        }

        private String getIndexName() {
            return task.getIndexName();
        }
    }
}
