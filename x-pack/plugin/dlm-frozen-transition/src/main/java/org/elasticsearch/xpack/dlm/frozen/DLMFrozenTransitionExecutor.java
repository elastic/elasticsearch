/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.dlm.DataStreamLifecycleErrorStore;
import org.elasticsearch.logging.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;

import static org.elasticsearch.logging.LogManager.getLogger;

/**
 * DLMFrozenTransitionExecutor is responsible for managing and executing tasks related to
 * frozen transitions in the distributed lifecycle management (DLM) feature.
 * <br>
 * This executor limits the number of concurrent transition tasks based on a configurable capacity
 * and prevents transitions being executed concurrently for the same index.
 * It also ensures that tasks are tracked and cleaned up upon completion or failure.
 */
class DLMFrozenTransitionExecutor {

    private static final Logger logger = getLogger(DLMFrozenTransitionExecutor.class);

    private final ExecutorService executor;
    private final int maxSubmitted;
    private final DataStreamLifecycleErrorStore errorStore;
    private final MasterServiceTaskQueue<UnmarkIndexForFrozenTask> unmarkIndexForDlmFrozenConversionQueue;
    private final DLMFrozenTransitionSettings frozenTransitionSettings;

    private volatile Map<String, Future<?>> submittedTransitions;
    private volatile boolean isAccepting = true;

    DLMFrozenTransitionExecutor(
        ClusterService clusterService,
        int maxSubmitted,
        DLMFrozenTransitionSettings frozenTransitionSettings,
        DataStreamLifecycleErrorStore errorStore,
        ExecutorService executor
    ) {
        this.maxSubmitted = maxSubmitted;
        this.submittedTransitions = Collections.synchronizedMap(new HashMap<>(maxSubmitted));
        this.executor = executor;
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
        return submittedTransitions.size() < maxSubmitted;
    }

    // We need the thread to be interrupted to prevent concurrent transitions on multiple nodes,
    // and original reason for fobidding this API (https://github.com/elastic/elasticsearch/pull/8494) does not apply in this case
    @SuppressForbidden(reason = "Future#cancel()")
    public synchronized void stop() {
        isAccepting = false;
        submittedTransitions.values().forEach(future -> future.cancel(true));
        submittedTransitions = Collections.synchronizedMap(new HashMap<>(maxSubmitted));
    }

    public synchronized void start() {
        isAccepting = true;
    }

    public synchronized Future<?> submit(DLMFrozenTransitionRunnable task) {
        final String indexName = task.getIndexName();
        if (isAccepting == false) {
            throw new RejectedExecutionException("DLM frozen executor is stopped");
        }
        FutureTask<?> futureTask = new FutureTask<>(wrapRunnable(task), null);
        Future<?> previousValue = submittedTransitions.put(indexName, futureTask);
        assert Objects.isNull(previousValue) : "expected the previous value be null, but it was " + previousValue;
        try {
            executor.execute(futureTask);
            return futureTask;
        } catch (Exception e) {
            submittedTransitions.remove(indexName);
            throw e;
        }
    }

    /**
     * Wraps the task with index tracking and error handling. Ensures the index name is always removed from
     * {@link #submittedTransitions} when the thread completes, whether successfully or with an error.
     * <p>
     * The current {@code submittedTransitions} map reference is captured here so that the wrapper's cleanup
     * removes the entry from the map the task was registered in. {@link #stop()} replaces the field with a
     * fresh map; if the wrapper re-read the field at completion time it could otherwise remove an entry
     * belonging to a different task submitted after a {@code stop()}/{@code start()} cycle.
     */
    private Runnable wrapRunnable(DLMFrozenTransitionRunnable task) {
        return new WrappedDlmFrozenTransitionRunnable(task, submittedTransitions);
    }

    public boolean isAccepting() {
        return isAccepting;
    }

    // Visible for testing
    DataStreamLifecycleErrorStore getErrorStore() {
        return errorStore;
    }

    // Visible for testing
    boolean hasSubmittedTransitions() {
        return submittedTransitions.isEmpty() == false;
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

    class WrappedDlmFrozenTransitionRunnable implements Runnable {
        private final DLMFrozenTransitionRunnable task;
        private final Map<String, Future<?>> transitionsMap;

        private WrappedDlmFrozenTransitionRunnable(DLMFrozenTransitionRunnable task, Map<String, Future<?>> transitionsMap) {
            this.task = task;
            this.transitionsMap = transitionsMap;
        }

        @Override
        public void run() {
            final String indexName = getIndexName();
            try {
                logger.debug("Starting transition for index [{}]", indexName);
                task.run();
                logger.debug("Transition completed for index [{}]", indexName);
                transitionsMap.remove(indexName);
            } catch (DLMUnrecoverableException err) {
                logger.debug(
                    "DLM encountered an unrecoverable error while converting [{}] "
                        + "to a frozen index, submitting task to unmark it for conversion",
                    indexName
                );
                unmarkIndexForDlmFrozenConversionQueue.submitTask(
                    "dlm-unmark-frozen-" + indexName,
                    new UnmarkIndexForFrozenTask(task.getProjectId(), task.getIndexName(), ActionListener.wrap(resp -> {
                        logger.debug("DLM successfully unmarked index [{}] for frozen conversion", indexName);
                        transitionsMap.remove(indexName);
                    }, exception -> {
                        errorStore.recordAndLogError(
                            task.getProjectId(),
                            indexName,
                            exception,
                            Strings.format("Error unmarking index [%s] for conversion to frozen index", indexName),
                            frozenTransitionSettings.getErrorRetryInterval()
                        );
                        transitionsMap.remove(indexName);
                    })),
                    null
                );
            } catch (Exception ex) {
                if (ExceptionsHelper.unwrap(ex, InterruptedException.class) != null || Thread.currentThread().isInterrupted()) {
                    Thread.currentThread().interrupt();
                    logger.debug("Transition for index [{}] was interrupted, skipping error recording", indexName);
                }
                if (DLMConvertToFrozen.isTransientMasterFailoverException(ex)) {
                    logger.debug(
                        "Transient master-failover exception during frozen transition for index [{}], will retry on next tick",
                        indexName,
                        ex
                    );
                } else {
                    errorStore.recordAndLogError(
                        task.getProjectId(),
                        indexName,
                        ex,
                        Strings.format("Error executing transition for index [%s]", indexName),
                        frozenTransitionSettings.getErrorRetryInterval()
                    );
                }
                transitionsMap.remove(indexName);
            }
        }

        String getIndexName() {
            return task.getIndexName();
        }
    }
}
