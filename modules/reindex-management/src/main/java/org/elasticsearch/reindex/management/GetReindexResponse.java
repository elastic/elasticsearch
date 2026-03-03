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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public class GetReindexResponse extends ActionResponse implements ToXContentObject {

    private final TaskResult task;

    /**
     * Matches a reindex description and captures only the safe fields we want to expose:
     * group(1) = optional safe remote info (scheme, host, port, pathPrefix), null for local reindex
     * group(2) = source indices
     * group(3) = destination index
     */
    private static final Pattern DESCRIPTION_PATTERN = Pattern.compile(
        "(?s)^reindex from (?:\\[((?:scheme=\\S+ )?host=\\S+ port=\\d+(?:\\s+pathPrefix=\\S+)?) .+\\])?\\[([^\\]]*)].*to \\[([^\\]]*)]$"
    );

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

    public TaskResult getTaskResult() {
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
        taskInfoToXContent(builder, params, taskInfo);
        if (task.getError() != null) {
            XContentHelper.writeRawField("error", task.getError(), builder.contentType(), builder, params);
        }
        if (task.getResponse() != null) {
            XContentHelper.writeRawField("response", task.getResponse(), builder.contentType(), builder, params);
        }
        builder.endObject();
        return builder;
    }

    // reindex specific TaskInfo serialization
    static XContentBuilder taskInfoToXContent(XContentBuilder builder, Params params, TaskInfo taskInfo) throws IOException {
        builder.field("id", taskInfo.node() + ":" + taskInfo.id());
        Optional<String> description = sanitizeDescription(taskInfo.description());
        if (description.isPresent()) {
            builder.field("description", description.get());
        }
        builder.timestampFieldsFromUnixEpochMillis("start_time_in_millis", "start_time", taskInfo.startTime());
        if (builder.humanReadable()) {
            builder.field("running_time", TimeValue.timeValueNanos(taskInfo.runningTimeNanos()).toString());
        }
        builder.field("running_time_in_nanos", taskInfo.runningTimeNanos());
        builder.field("cancelled", taskInfo.cancelled());
        if (taskInfo.status() != null) {
            builder.field("status", taskInfo.status(), params);
        }

        return builder;
    }

    /**
     * Selectively constructs a safe description by extracting only the fields we want to expose and discarding everything else.
     * Returns empty if the description cannot be parsed, so we don't risk exposing sensitive data from an unrecognised format.
     */
    static Optional<String> sanitizeDescription(String description) {
        if (description == null) {
            return Optional.empty();
        }
        Matcher matcher = DESCRIPTION_PATTERN.matcher(description);
        if (matcher.matches()) {
            String remoteInfo = matcher.group(1);
            String sourceIndices = matcher.group(2);
            String destIndex = matcher.group(3);
            StringBuilder sb = new StringBuilder("reindex from ");
            if (remoteInfo != null) {
                sb.append('[').append(remoteInfo).append(']');
            }
            sb.append('[').append(sourceIndices).append("] to [").append(destIndex).append(']');
            return Optional.of(sb.toString());
        }
        return Optional.empty();
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
