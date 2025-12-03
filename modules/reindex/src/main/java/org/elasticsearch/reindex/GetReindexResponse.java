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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Response for getting a reindex operation.
 */
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
     * Create a GetReindexResponse from a TaskResult.
     */
    public static GetReindexResponse fromTaskResult(TaskResult taskResult) {
        return new GetReindexResponse(taskResult);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        // Only expose selected fields, hide task implementation details
        builder.field("completed", task.isCompleted());
        if (task.getError() != null) {
            XContentHelper.writeRawField("error", task.getError(), XContentType.JSON, builder, params);
        }
        if (task.getResponse() != null) {
            XContentHelper.writeRawField("response", task.getResponse(), XContentType.JSON, builder, params);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
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
