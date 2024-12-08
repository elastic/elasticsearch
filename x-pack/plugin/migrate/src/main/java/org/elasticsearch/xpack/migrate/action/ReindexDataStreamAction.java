/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

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
            super(in);
            this.taskId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(taskId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("task", getTaskId());
            builder.endObject();
            return builder;
        }

        public String getTaskId() {
            return taskId;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(taskId);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ReindexDataStreamResponse && taskId.equals(((ReindexDataStreamResponse) other).taskId);
        }

    }

    public static class ReindexDataStreamRequest extends ActionRequest {
        private final String sourceDataStream;

        public ReindexDataStreamRequest(String sourceDataStream) {
            super();
            this.sourceDataStream = sourceDataStream;
        }

        public ReindexDataStreamRequest(StreamInput in) throws IOException {
            super(in);
            this.sourceDataStream = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sourceDataStream);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean getShouldStoreResult() {
            return true; // do not wait_for_completion
        }

        public String getSourceDataStream() {
            return sourceDataStream;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(sourceDataStream);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof ReindexDataStreamRequest
                && sourceDataStream.equals(((ReindexDataStreamRequest) other).sourceDataStream);
        }
    }
}
