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
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class GetReindexDataStreamStatusAction extends ActionType<GetReindexDataStreamStatusAction.Response> {

    public static final GetReindexDataStreamStatusAction INSTANCE = new GetReindexDataStreamStatusAction();
    public static final String NAME = "indices:admin/data_stream/reindex_status";

    public GetReindexDataStreamStatusAction() {
        super(NAME);
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private final TaskResult task;

        public Response(TaskResult task) {
            this.task = requireNonNull(task, "task is required");
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            task = in.readOptionalWriteable(TaskResult::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalWriteable(task);
        }

        /**
         * Get the actual result of the fetch.
         */
        public TaskResult getTask() {
            return task;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            task.getTask().status().toXContent(builder, params);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(task);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Response && task.equals(((Response) other).task);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }

    }

    public static class Request extends ActionRequest implements IndicesRequest {
        private final String persistentTaskId;

        public Request(String persistentTaskId) {
            super();
            this.persistentTaskId = persistentTaskId;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.persistentTaskId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(persistentTaskId);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean getShouldStoreResult() {
            return true; // do not wait_for_completion
        }

        public String getPersistentTaskId() {
            return persistentTaskId;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(persistentTaskId);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Request && persistentTaskId.equals(((Request) other).persistentTaskId);
        }

        public Request nodeRequest(String thisNodeId, long thisTaskId) {
            Request copy = new Request(persistentTaskId);
            copy.setParentTask(thisNodeId, thisTaskId);
            return copy;
        }

        @Override
        public String[] indices() {
            return new String[] { persistentTaskId.substring("reindex-data-stream-".length()) };
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictSingleIndexNoExpandForbidClosed();
        }
    }
}
