/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.task;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ReindexDataStreamTask extends AllocatedPersistentTask {
    public static final String TASK_NAME = "reindex-data-stream";
    private final ClusterService clusterService;
    private final long persistentTaskStartTime;
    private final int initialTotalIndices;
    private final int initialTotalIndicesToBeUpgraded;
    private boolean isCompleteLocally = false;
    private volatile Exception exception;
    private final Set<String> inProgress = Collections.synchronizedSet(new HashSet<>());
    private final AtomicInteger pending = new AtomicInteger();
    private final List<Tuple<String, Exception>> errors = Collections.synchronizedList(new ArrayList<>());
    private final RunOnce completeTask;

    @SuppressWarnings("this-escape")
    public ReindexDataStreamTask(
        ClusterService clusterService,
        long persistentTaskStartTime,
        int initialTotalIndices,
        int initialTotalIndicesToBeUpgraded,
        long id,
        String type,
        String action,
        String description,
        TaskId parentTask,
        Map<String, String> headers
    ) {
        super(id, type, action, description, parentTask, headers);
        this.clusterService = clusterService;
        this.persistentTaskStartTime = persistentTaskStartTime;
        this.initialTotalIndices = initialTotalIndices;
        this.initialTotalIndicesToBeUpgraded = initialTotalIndicesToBeUpgraded;
        this.pending.set(initialTotalIndicesToBeUpgraded);
        this.completeTask = new RunOnce(() -> {
            if (exception == null) {
                markAsCompleted();
            } else {
                markAsFailed(exception);
            }
        });
    }

    @Override
    public ReindexDataStreamStatus getStatus() {
        int totalIndices = initialTotalIndices;
        int totalIndicesToBeUpgraded = initialTotalIndicesToBeUpgraded;
        PersistentTasksCustomMetadata.PersistentTask<?> persistentTask = PersistentTasksCustomMetadata.getTaskWithId(
            clusterService.state(),
            getPersistentTaskId()
        );
        boolean isComplete;
        if (persistentTask != null) {
            ReindexDataStreamPersistentTaskState state = (ReindexDataStreamPersistentTaskState) persistentTask.getState();
            if (state != null) {
                isComplete = state.isComplete();
                if (state.totalIndices() != null && state.totalIndicesToBeUpgraded() != null) {
                    totalIndices = Math.toIntExact(state.totalIndices());
                    totalIndicesToBeUpgraded = Math.toIntExact(state.totalIndicesToBeUpgraded());
                }
            } else {
                isComplete = false;
            }
        } else {
            isComplete = false;
        }
        return new ReindexDataStreamStatus(
            persistentTaskStartTime,
            totalIndices,
            totalIndicesToBeUpgraded,
            isComplete,
            exception,
            inProgress,
            pending.get(),
            errors
        );
    }

    public void allReindexesCompleted(ThreadPool threadPool, TimeValue timeToLive) {
        isCompleteLocally = true;
        if (isCancelled()) {
            completeTask.run();
        } else {
            threadPool.schedule(completeTask, timeToLive, threadPool.generic());
        }
    }

    public void taskFailed(ThreadPool threadPool, TimeValue timeToLive, Exception e) {
        this.exception = e;
        allReindexesCompleted(threadPool, timeToLive);
    }

    public void reindexSucceeded(String index) {
        inProgress.remove(index);
    }

    public void reindexFailed(String index, Exception error) {
        this.errors.add(Tuple.tuple(index, error));
        inProgress.remove(index);
    }

    public void incrementInProgressIndicesCount(String index) {
        inProgress.add(index);
        pending.decrementAndGet();
    }

    private boolean isCompleteInClusterState() {
        PersistentTasksCustomMetadata persistentTasksCustomMetadata = clusterService.state()
            .getMetadata()
            .getProject()
            .custom(PersistentTasksCustomMetadata.TYPE);
        PersistentTasksCustomMetadata.PersistentTask<?> persistentTask = persistentTasksCustomMetadata.getTask(getPersistentTaskId());
        if (persistentTask != null) {
            ReindexDataStreamPersistentTaskState state = (ReindexDataStreamPersistentTaskState) persistentTask.getState();
            if (state != null) {
                return state.isComplete();
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    public void setPendingIndicesCount(int size) {
        pending.set(size);
    }

    @Override
    public void onCancelled() {
        /*
         * If the task is complete, but just waiting for its scheduled removal, we go ahead and call markAsCompleted/markAsFailed
         * immediately. This results in the running task being removed from the task manager. If the task is not complete, then one of
         * allReindexesCompleted or taskFailed will be called in the future, resulting in the same thing.
         * We check both the cluster state and isCompleteLocally -- it is possible (especially in tests) that hte cluster state
         * update has not happened in between when allReindexesCompleted was called and when this is called.
         */
        if (isCompleteInClusterState() || isCompleteLocally) {
            completeTask.run();
        }
    }
}
