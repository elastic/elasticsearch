/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.task;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ReindexDataStreamTask extends AllocatedPersistentTask {
    public static final String TASK_NAME = "reindex-data-stream";
    private final long persistentTaskStartTime;
    private final int totalIndices;
    private final int totalIndicesToBeUpgraded;
    private boolean complete = false;
    private Exception exception;
    private AtomicInteger inProgress = new AtomicInteger(0);
    private AtomicInteger pending = new AtomicInteger();
    private List<Tuple<String, Exception>> errors = new ArrayList<>();

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
