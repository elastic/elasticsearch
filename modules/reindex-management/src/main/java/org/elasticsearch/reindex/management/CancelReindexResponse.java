/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Response returned from {@code POST /_reindex/{taskId}/_cancel}.
 */
public class CancelReindexResponse extends BaseTasksResponse implements ToXContentObject {

    @Nullable
    private final GetReindexResponse completedReindexResponse;

    public CancelReindexResponse(
        final List<TaskOperationFailure> taskFailures,
        final List<FailedNodeException> failedNodes,
        @Nullable final GetReindexResponse completedReindexResponse
    ) {
        super(taskFailures, failedNodes);
        this.completedReindexResponse = completedReindexResponse;
    }

    public CancelReindexResponse(final StreamInput in) throws IOException {
        super(in);
        this.completedReindexResponse = in.readOptionalWriteable(GetReindexResponse::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(completedReindexResponse);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        assert getNodeFailures().isEmpty() && getTaskFailures().isEmpty() : "should be thrown before being able to call serialization";
        if (completedReindexResponse != null) {
            return completedReindexResponse.toXContent(builder, params);
        }
        builder.startObject();
        builder.field("acknowledged", true);
        return builder.endObject();
    }

    public Optional<GetReindexResponse> getCompletedReindexResponse() {
        return Optional.ofNullable(completedReindexResponse);
    }
}
