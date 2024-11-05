/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class ReindexDataStreamAction extends ActionType<ReindexDataStreamAction.ReindexDataStreamResponse> {

    public static final ReindexDataStreamAction INSTANCE = new ReindexDataStreamAction();
    public static final String NAME = "indices:admin/data_stream/reindex";

    public ReindexDataStreamAction() {
        super(NAME);
    }

    public static class ReindexDataStreamResponse extends ActionResponse implements ToXContentObject {
        private final String taskId;

        public ReindexDataStreamResponse(String taskId) {
            super();
            this.taskId = taskId;
        }

        public ReindexDataStreamResponse(StreamInput in) throws IOException {
            this.taskId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(taskId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("successes", List.of("one", "two"));
            builder.field("errors", List.of("one", "two"));
            builder.endObject();
            return builder;
        }

        public String getTaskId() {
            return taskId;
        }
    }

    public static class ReindexDataStreamRequest extends ActionRequest {
        private final String sourceIndex;

        public ReindexDataStreamRequest(String sourceIndex) {
            super();
            this.sourceIndex = sourceIndex;
        }

        public ReindexDataStreamRequest(StreamInput in) throws IOException {
            super(in);
            this.sourceIndex = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sourceIndex);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean getShouldStoreResult() {
            return true; // not wait_for_completion
        }

        public String getSourceIndex() {
            return sourceIndex;
        }
    }
}
