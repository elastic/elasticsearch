/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskResult;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Represents the user-facing view of a reindex task that might have been relocated to a different node.
 * All information takes relocations into account and produces a consistent user-facing view of the task.
 */
public record RelocatableReindexResult(TaskResult original, @Nullable TaskResult relocated) implements Writeable {
    public RelocatableReindexResult {
        Objects.requireNonNull(original, "original task is required");
    }

    public RelocatableReindexResult(final StreamInput in) throws IOException {
        this(new TaskResult(in), in.readOptionalWriteable(TaskResult::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeWriteable(original);
        out.writeOptionalWriteable(relocated);
    }

    public RelocatableReindexResult withLatestRelocatedTask(final TaskResult relocated) {
        Objects.requireNonNull(relocated, "relocated task cannot be null");
        return new RelocatableReindexResult(original, relocated);
    }

    public RelocatableReindexResult withUpdatedResult(final TaskResult updatedResult) {
        Objects.requireNonNull(updatedResult, "updated result cannot be null");
        return relocated == null
            ? new RelocatableReindexResult(updatedResult, null)
            : new RelocatableReindexResult(original, updatedResult);
    }

    public TaskResult latestTask() {
        return relocated != null ? relocated : original;
    }

    public Task.Status status() {
        return latestTask().getTask().status();
    }

    public String description() {
        return original.getTask().description();
    }

    public long startTimeMillis() {
        return original.getTask().startTime();
    }

    public long runningTimeNanos() {
        return relocated == null
            ? original.getTask().runningTimeNanos()
            : relocated.getTask().runningTimeNanos() + TimeUnit.MILLISECONDS.toNanos(
                relocated.getTask().startTime() - original.getTask().startTime()
            );
    }

    public TaskResult original() {
        return original;
    }

    public Optional<TaskResult> relocatedTask() {
        return Optional.ofNullable(relocated);
    }

    public TaskId latestTaskId() {
        return latestTask().getTask().taskId();
    }

    public TaskId originalTaskId() {
        return original.getTask().taskId();
    }

    public boolean isCompleted() {
        return latestTask().isCompleted();
    }

    public boolean isCancelled() {
        return latestTask().getTask().cancelled();
    }

    public BytesReference error() {
        return latestTask().getError();
    }

    public Map<String, Object> errorAsMap() {
        return latestTask().getErrorAsMap();
    }

    public BytesReference response() {
        return latestTask().getResponse();
    }
}
