/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.task;

import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class ReindexDataStreamTask extends AllocatedPersistentTask {
    public static final String TASK_NAME = "reindex-data-stream";
    private final long persistentTaskStartTime;
    private final int totalIndices;
    private final int totalIndicesToBeUpgraded;
    private boolean complete = false;
    private Exception exception;
    private final AtomicInteger inProgress = new AtomicInteger(0);
    private final AtomicInteger pending = new AtomicInteger();
    private final List<Tuple<String, Exception>> errors = new ArrayList<>();
    private final RunOnce completeTask;

    /*
     * This lock is used to make sure that we don't have a race condition where the task completes and is cancelled at the same time, which
     * without this lock could lead to the task never being removed from the task manager (done in markAsCompleted).
     */
    private final ReentrantLock completionAndCancellationLock = new ReentrantLock();

    @SuppressWarnings("this-escape")
    public ReindexDataStreamTask(
        long persistentTaskStartTime,
        int totalIndices,
        int totalIndicesToBeUpgraded,
        long id,
        String type,
        String action,
        String description,
        TaskId parentTask,
        Map<String, String> headers
    ) {
        super(id, type, action, description, parentTask, headers);
        this.persistentTaskStartTime = persistentTaskStartTime;
        this.totalIndices = totalIndices;
        this.totalIndicesToBeUpgraded = totalIndicesToBeUpgraded;
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
        return new ReindexDataStreamStatus(
            persistentTaskStartTime,
            totalIndices,
            totalIndicesToBeUpgraded,
            complete,
            exception,
            inProgress.get(),
            pending.get(),
            errors
        );
    }

    public void allReindexesCompleted(ThreadPool threadPool, TimeValue timeToLive) {
        completionAndCancellationLock.lock();
        try {
            this.complete = true;
            if (isCancelled()) {
                completeTask.run();
            } else {
                threadPool.schedule(completeTask, timeToLive, threadPool.generic());
            }
        } finally {
            completionAndCancellationLock.unlock();
        }
    }

    public void taskFailed(ThreadPool threadPool, TimeValue timeToLive, Exception e) {
        this.exception = e;
        allReindexesCompleted(threadPool, timeToLive);
    }

    public void reindexSucceeded() {
        inProgress.decrementAndGet();
    }

    public void reindexFailed(String index, Exception error) {
        this.errors.add(Tuple.tuple(index, error));
        inProgress.decrementAndGet();
    }

    public void incrementInProgressIndicesCount() {
        inProgress.incrementAndGet();
        pending.decrementAndGet();
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
         */
        completionAndCancellationLock.lock();
        try {
            if (complete) {
                completeTask.run();
            }
        } finally {
            completionAndCancellationLock.unlock();
        }
    }
}
