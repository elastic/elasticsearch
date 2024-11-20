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

public class ReindexDataStreamTask extends AllocatedPersistentTask {
    public static final String TASK_NAME = "reindex-data-stream";
    private final long persistentTaskStartTime;
    private final int totalIndices;
    private final int totalIndicesToBeUpgraded;
    private final ThreadPool threadPool;
    private boolean complete = false;
    private Exception exception;
    private List<String> inProgress = new ArrayList<>();
    private List<String> pending = List.of();
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
            inProgress.size(),
            pending.size(),
            errors
        );
    }

    public void reindexSucceeded() {
        this.complete = true;
    }

    public void reindexFailed(Exception e) {
        this.complete = true;
        this.exception = e;
    }

    public void setInProgressIndices(List<String> inProgressIndices) {
        this.inProgress = inProgressIndices;
    }

    public void setPendingIndices(List<String> pendingIndices) {
        this.pending = pendingIndices;
    }

    public void addErrorIndex(String index, Exception error) {
        this.errors.add(Tuple.tuple(index, error));
    }
}
