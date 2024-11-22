/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.task;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ReindexDataStreamTask extends AllocatedPersistentTask {
    public static final String TASK_NAME = "reindex-data-stream";
    private final long persistentTaskStartTime;
    private final int totalIndices;
    private final int totalIndicesToBeUpgraded;
    private final ThreadPool threadPool;
    private boolean complete = false;
    private Exception exception;
    private AtomicInteger inProgress = new AtomicInteger(0);
    private AtomicInteger pending = new AtomicInteger();
    private List<Tuple<String, Exception>> errors = new ArrayList<>();

    public ReindexDataStreamTask(
        long persistentTaskStartTime,
        int totalIndices,
        int totalIndicesToBeUpgraded,
        ThreadPool threadPool,
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
        this.threadPool = threadPool;
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

    public void allReindexesCompleted() {
        this.complete = true;
    }

    public void taskFailed(Exception e) {
        this.complete = true;
        this.exception = e;
    }

    public void reindexSucceeded() {
        inProgress.decrementAndGet();
    }

    public void reindexFailed(String index, Exception error) {
        System.out.println("****************** In ReindexDataStreamTask *************************** ");
        error.printStackTrace(System.out);
        new RuntimeException("****************** In ReindexDataStreamTask ***************************").printStackTrace(System.out);
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
}
