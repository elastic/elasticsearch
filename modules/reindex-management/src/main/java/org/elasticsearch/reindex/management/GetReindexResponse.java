/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v 3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class GetReindexResponse extends ActionResponse implements ToXContentObject {

    /** The original task the user submitted. Always present. */
    private final TaskResult originalTask;
    /** The final task after relocation, or {@code null} if the task was never relocated. */
    @Nullable
    private final TaskResult relocatedTask;

    public GetReindexResponse(TaskResult originalTask) {
        this(originalTask, null);
    }

    public GetReindexResponse(TaskResult originalTask, @Nullable TaskResult relocatedTask) {
        this.originalTask = requireNonNull(originalTask, "task is required");
        this.relocatedTask = relocatedTask;
    }

    public GetReindexResponse(StreamInput in) throws IOException {
        originalTask = in.readOptionalWriteable(TaskResult::new);
        relocatedTask = in.readOptionalWriteable(TaskResult::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(originalTask);
        out.writeOptionalWriteable(relocatedTask);
    }

    public TaskResult getOriginalTask() {
        return originalTask;
    }

    public Optional<TaskResult> getRelocatedTask() {
        return Optional.ofNullable(relocatedTask);
    }

    /**
     * Only selected fields are exposed, to hide task related implementation details.
     * When relocation occurred, ID and timing fields reflect the original task
     * while the completion state comes from the relocated task.
     */
    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        final TaskResult finalTask = relocatedTask == null ? originalTask : relocatedTask;
        builder.startObject();
        builder.field("completed", finalTask.isCompleted());
        taskInfoToXContent(builder, params, originalTask.getTask(), relocatedTask == null ? null : relocatedTask.getTask());
        if (finalTask.getError() != null) {
            XContentHelper.writeRawField("error", finalTask.getError(), builder.contentType(), builder, params);
        }
        if (finalTask.getResponse() != null) {
            XContentHelper.writeRawField("response", finalTask.getResponse(), builder.contentType(), builder, params);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Renders reindex-specific task info fields.
     * Always provides the user-facing id and start time.
     * When {@code relocatedTaskInfo} is non-null, running time accounts for the gap between original and relocated
     * start, and cancelled and status reflect the relocated task.
     */
    static XContentBuilder taskInfoToXContent(
        final XContentBuilder builder,
        final Params params,
        final TaskInfo originalTaskInfo,
        final @Nullable TaskInfo relocatedTaskInfo
    ) throws IOException {
        final TaskInfo finalTaskInfo = relocatedTaskInfo == null ? originalTaskInfo : relocatedTaskInfo;
        final long reindexStartMillis = originalTaskInfo.startTime();
        final long reindexRunningTimeNanos = relocatedTaskInfo == null
            ? originalTaskInfo.runningTimeNanos()
            : finalTaskInfo.runningTimeNanos() + TimeUnit.MILLISECONDS.toNanos(finalTaskInfo.startTime() - reindexStartMillis);

        // TODO: revisit if we should expose taskInfo.description, since it may contain sensitive information like ip and username
        builder.field("id", originalTaskInfo.node() + ":" + originalTaskInfo.id());
        builder.timestampFieldsFromUnixEpochMillis("start_time_in_millis", "start_time", reindexStartMillis);
        if (builder.humanReadable()) {
            builder.field("running_time", TimeValue.timeValueNanos(reindexRunningTimeNanos).toString());
        }
        builder.field("running_time_in_nanos", reindexRunningTimeNanos);
        builder.field("cancelled", finalTaskInfo.cancelled());
        if (finalTaskInfo.status() != null) {
            builder.field("status", finalTaskInfo.status(), params);
        }

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetReindexResponse that = (GetReindexResponse) o;
        return Objects.equals(originalTask, that.originalTask) && Objects.equals(relocatedTask, that.relocatedTask);
    }

    @Override
    public int hashCode() {
        return Objects.hash(originalTask, relocatedTask);
    }
}
