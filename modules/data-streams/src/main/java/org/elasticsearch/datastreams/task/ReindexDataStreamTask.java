/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.task;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReindexDataStreamTask extends AllocatedPersistentTask {
    public static final String TASK_NAME = "reindex-data-stream";
    private static final TimeValue TASK_KEEP_ALIVE_TIME = TimeValue.timeValueDays(1);
    private final ThreadPool threadPool;
    private boolean complete = false;
    private Exception exception;
    private List<String> sucesses = new ArrayList<>();
    private List<String> inProgress = new ArrayList<>();
    private List<String> pending = List.of();
    private Map<String, Exception> errors = new HashMap<>();

    public ReindexDataStreamTask(
        ThreadPool threadPool,
        long id,
        String type,
        String action,
        String description,
        TaskId parentTask,
        Map<String, String> headers
    ) {
        super(id, type, action, description, parentTask, headers);
        this.threadPool = threadPool;
    }

    @Override
    public ReindexDataStreamStatus getStatus() {
        return new ReindexDataStreamStatus(complete, exception, sucesses, inProgress, pending, errors);
    }

    @Override
    public void markAsCompleted() {
        this.complete = true;
        threadPool.schedule(super::markAsCompleted, TASK_KEEP_ALIVE_TIME, threadPool.generic());
    }

    @Override
    public void markAsFailed(Exception e) {
        this.complete = true;
        this.exception = e;
        threadPool.schedule(() -> super.markAsFailed(e), TASK_KEEP_ALIVE_TIME, threadPool.generic());
    }

    public void addSuccessfulIndex(String index) {
        this.sucesses.add(index);
    }

    public void setInProgressIndices(List<String> inProgressIndices) {
        this.inProgress = inProgressIndices;
    }

    public void setPendingIndices(List<String> pendingIndices) {
        this.pending = pendingIndices;
    }

    public void addErrorIndex(String index, Exception error) {
        this.errors.put(index, error);
    }
}
