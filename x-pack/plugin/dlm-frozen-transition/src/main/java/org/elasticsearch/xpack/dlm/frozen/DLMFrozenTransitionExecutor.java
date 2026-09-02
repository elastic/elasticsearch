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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.datastreams.lifecycle.FrozenTransitionInfoProvider;
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

    private volatile Map<TransitionKey, SubmittedTransition> submittedTransitions;
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

    public boolean transitionSubmitted(ProjectId projectId, String indexName) {
        return submittedTransitions.containsKey(new TransitionKey(projectId, indexName));
    }

    public boolean hasCapacity() {
        return submittedTransitions.size() < maxSubmitted;
    }

    /**
     * Returns the current execution status of the frozen tier transition for the given index, as tracked by this
     * executor. An index with no submitted transition is reported as {@link FrozenTransitionInfoProvider.Status#NOT_STARTED}.
     */
    public FrozenTransitionInfoProvider.Status getTransitionStatus(ProjectId projectId, String indexName) {
        SubmittedTransition submitted = submittedTransitions.get(new TransitionKey(projectId, indexName));
        return submitted == null ? FrozenTransitionInfoProvider.Status.NOT_STARTED : submitted.tracker().status;
    }

    // We need the thread to be interrupted to prevent concurrent transitions on multiple nodes,
    // and original reason for fobidding this API (https://github.com/elastic/elasticsearch/pull/8494) does not apply in this case
    @SuppressForbidden(reason = "Future#cancel()")
    public synchronized void stop() {
        isAccepting = false;
        submittedTransitions.values().forEach(submitted -> submitted.future().cancel(true));
        submittedTransitions = Collections.synchronizedMap(new HashMap<>(maxSubmitted));
    }

    public synchronized void start() {
        isAccepting = true;
    }

    public synchronized Future<?> submit(DLMFrozenTransitionRunnable task) {
        final TransitionKey key = new TransitionKey(task.getProjectId(), task.getIndexName());
        if (isAccepting == false) {
            throw new RejectedExecutionException("DLM frozen executor is stopped");
        }
        TransitionTracker tracker = new TransitionTracker();
        FutureTask<?> futureTask = new FutureTask<>(new WrappedDlmFrozenTransitionRunnable(task, submittedTransitions, tracker), null);
        SubmittedTransition previousValue = submittedTransitions.put(key, new SubmittedTransition(futureTask, tracker));
        assert Objects.isNull(previousValue) : "expected the previous value be null, but it was " + previousValue;
        try {
            executor.execute(futureTask);
            return futureTask;
        } catch (Exception e) {
            submittedTransitions.remove(key);
            throw e;
        }
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

    /**
     * Identifies a submitted transition by the project and index it belongs to. Multiple projects can contain
     * indices with the same name, so the index name alone is not a safe map key.
     */
    private record TransitionKey(ProjectId projectId, String indexName) {}

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

    /**
     * A single submitted transition as held in {@link #submittedTransitions}: the {@link Future} used to cancel
     * the task on {@link #stop()}, plus the {@link TransitionTracker} exposing its execution status.
     * <p>
     * Keeping the future here rather than on the tracker breaks what would otherwise be a reference cycle
     * ({@code future -> runnable -> tracker -> future}): the running task references only the tracker, and this
     * value is constructed in {@link #submit} after the {@link FutureTask} already exists, so no field needs to
     * be back-patched.
     */
    private record SubmittedTransition(Future<?> future, TransitionTracker tracker) {}

    /**
     * Tracks the execution status of a single submitted transition: queued when submitted, running once the task
     * starts. Mutated by the running task via {@link WrappedDlmFrozenTransitionRunnable}.
     */
    static final class TransitionTracker {
        volatile FrozenTransitionInfoProvider.Status status = FrozenTransitionInfoProvider.Status.QUEUED;
    }

    /**
     * Wraps the submitted task with index tracking and error handling. Ensures the entry is always removed from
     * {@link #submittedTransitions} when the thread completes, whether successfully or with an error.
     * <p>
     * The current {@code submittedTransitions} map reference is captured here (via the constructor argument) so
     * that the wrapper's cleanup removes the entry from the map the task was registered in. {@link #stop()}
     * replaces the field with a fresh map; if the wrapper re-read the field at completion time it could otherwise
     * remove an entry belonging to a different task submitted after a {@code stop()}/{@code start()} cycle.
     * <p>
     * The tracker must be passed in explicitly rather than looked up from the map: the wrapper is constructed
     * in {@link #submit} before the tracker has been put into {@code submittedTransitions}, so a lookup at
     * construction time would find nothing.
     */
    class WrappedDlmFrozenTransitionRunnable implements Runnable {
        private final DLMFrozenTransitionRunnable task;
        private final Map<TransitionKey, SubmittedTransition> transitionsMap;
        private final TransitionTracker tracker;
        private final TransitionKey key;

        private WrappedDlmFrozenTransitionRunnable(
            DLMFrozenTransitionRunnable task,
            Map<TransitionKey, SubmittedTransition> transitionsMap,
            TransitionTracker tracker
        ) {
            this.task = task;
            this.transitionsMap = transitionsMap;
            this.tracker = tracker;
            this.key = new TransitionKey(task.getProjectId(), task.getIndexName());
        }

        @Override
        public void run() {
            final String indexName = getIndexName();
            tracker.status = FrozenTransitionInfoProvider.Status.RUNNING;
            try {
                logger.debug("Starting transition for index [{}]", indexName);
                task.run();
                logger.debug("Transition completed for index [{}]", indexName);
                transitionsMap.remove(key);
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
                        transitionsMap.remove(key);
                    }, exception -> {
                        errorStore.recordAndLogError(
                            task.getProjectId(),
                            indexName,
                            exception,
                            Strings.format("Error unmarking index [%s] for conversion to frozen index", indexName),
                            frozenTransitionSettings.getErrorRetryInterval()
                        );
                        transitionsMap.remove(key);
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
                transitionsMap.remove(key);
            }
        }

        String getIndexName() {
            return task.getIndexName();
        }
    }
}
