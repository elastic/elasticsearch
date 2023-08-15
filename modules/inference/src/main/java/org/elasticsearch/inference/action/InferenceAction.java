/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.results.InferenceResult;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class InferenceAction extends ActionType<InferenceAction.Response> {

    public static final InferenceAction INSTANCE = new InferenceAction();
    public static final String NAME = "cluster:monitor/inference";

    public InferenceAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        private final TaskType taskType;
        private final String modelId;

        public Request(TaskType taskType, String modelId) {
            this.taskType = taskType;
            this.modelId = modelId;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.taskType = TaskType.fromStream(in);
            this.modelId = in.readString();
        }

        public TaskType getTaskType() {
            return taskType;
        }

        public String getModelId() {
            return modelId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            taskType.writeTo(out);
            out.writeString(modelId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return taskType == request.taskType && Objects.equals(modelId, request.modelId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskType, modelId);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final InferenceResult result;

        public Response(InferenceResult result) {
            this.result = result;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            result = in.readNamedWriteable(InferenceResult.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteable(result);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            result.toXContent(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(result, response.result);
        }

        @Override
        public int hashCode() {
            return Objects.hash(result);
        }
    }

}
