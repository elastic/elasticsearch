/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class GetReindexResponse extends ActionResponse implements ToXContentObject {

    private final TaskResult task;

    public GetReindexResponse(TaskResult task) {
        this.task = requireNonNull(task, "task is required");
    }

    public GetReindexResponse(StreamInput in) throws IOException {
        task = in.readOptionalWriteable(TaskResult::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(task);
    }

    public TaskResult getTask() {
        return task;
    }

    /**
     * Only selected fields are exposed, to hide task related implementation details
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        TaskInfo taskInfo = task.getTask();
        builder.startObject();
        builder.field("completed", task.isCompleted());
        builder.field("description", taskInfo.description());
        builder.timestampFieldsFromUnixEpochMillis("start_time_in_millis", "start_time", taskInfo.startTime());
        if (builder.humanReadable()) {
            builder.field("running_time", TimeValue.timeValueNanos(taskInfo.runningTimeNanos()).toString());
        }
        builder.field("running_time_in_nanos", taskInfo.runningTimeNanos());
        builder.field("cancelled", taskInfo.cancelled());
        builder.field("status", taskInfo.status(), params);
        if (task.getError() != null) {
            XContentHelper.writeRawField("error", task.getError(), builder.contentType(), builder, params);
        }
        if (task.getResponse() != null) {
            XContentHelper.writeRawField("response", task.getResponse(), builder.contentType(), builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetReindexResponse that = (GetReindexResponse) o;
        return Objects.equals(task, that.task);
    }

    @Override
    public int hashCode() {
        return Objects.hash(task);
    }
}
