/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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

public class GetReindexResponse extends ActionResponse implements ToXContentObject {

    private final TaskResult taskResult;

    /**
     * Matches a reindex description and captures only the safe fields we want to expose:
     * group(1) = optional safe remote info (scheme, host, port, pathPrefix), null for local reindex
     * group(2) = source indices
     * group(3) = destination index
     */
    private static final Pattern DESCRIPTION_PATTERN = Pattern.compile("(?s)^reindex from " +
    // group(1): optional remote info
        "(?:\\[((?:scheme=\\S+ )?host=\\S+ port=\\d+(?:\\s+pathPrefix=\\S+)?)(?: .+)?\\])?" +
        // group(2): source indices
        "\\[([^\\]]*)].*" +
        // group(3): destination index
        "to \\[([^\\]]*)]$");

    public GetReindexResponse(final TaskResult taskResult) {
        this.taskResult = Objects.requireNonNull(taskResult, "taskResult is required");
    }

    public GetReindexResponse(StreamInput in) throws IOException {
        this(new TaskResult(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        taskResult.writeTo(out);
    }

    public TaskResult getTaskResult() {
        return taskResult;
    }

    /**
     * Only selected fields are exposed, to hide task related implementation details.
     * If relocation occurred, the Get Task API already merged timing so the result reflects the full duration.
     */
    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field("completed", taskResult.isCompleted());
        taskInfoToXContent(builder, params, taskResult.getTask());
        if (taskResult.getError() != null) {
            XContentHelper.writeRawField("error", taskResult.getError(), builder.contentType(), builder, params);
        }
        if (taskResult.getResponse() != null) {
            XContentHelper.writeRawField("response", taskResult.getResponse(), builder.contentType(), builder, params);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Renders reindex-specific task info fields.
     * Uses {@link TaskInfo#originalTaskId()} as the user-facing identity so that the originally-returned task ID is always shown,
     * even after relocation. Start time and running time are already adjusted by the relocation-aware Get Task API.
     */
    static XContentBuilder taskInfoToXContent(final XContentBuilder builder, final Params params, final TaskInfo info) throws IOException {
        builder.field("id", info.originalTaskId().toString());
        Optional<String> description = sanitizeDescription(info.description());
        if (description.isPresent()) {
            builder.field("description", description.get());
        }
        builder.timestampFieldsFromUnixEpochMillis("start_time_in_millis", "start_time", info.startTime());
        if (builder.humanReadable()) {
            builder.field("running_time", TimeValue.timeValueNanos(info.runningTimeNanos()).toString());
        }
        builder.field("running_time_in_nanos", info.runningTimeNanos());
        builder.field("cancelled", info.cancelled());
        if (info.status() != null) {
            builder.field("status", info.status(), params);
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
    public String toString() {
        return "GetReindexResponse{result=" + taskResult + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetReindexResponse that = (GetReindexResponse) o;
        return Objects.equals(taskResult, that.taskResult);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskResult);
    }
}
