/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.TaskType;
import org.elasticsearch.xpack.inference.results.InferenceResult;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class InferenceAction extends ActionType<InferenceAction.Response> {

    public static final InferenceAction INSTANCE = new InferenceAction();
    public static final String NAME = "cluster:monitor/xpack/inference";

    public InferenceAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        public static final ParseField INPUT = new ParseField("input");
        public static final ParseField TASK_SETTINGS = new ParseField("task_settings");

        static final ObjectParser<Request.Builder, Void> PARSER = new ObjectParser<>(NAME, Request.Builder::new);
        static {
            // TODO timeout
            PARSER.declareString(Request.Builder::setInput, INPUT);
            PARSER.declareObject(Request.Builder::setTaskSettings, (p, c) -> p.mapOrdered(), TASK_SETTINGS);
        }

        public static Request parseRequest(String modelId, String taskType, XContentParser parser) {
            Request.Builder builder = PARSER.apply(parser, null);
            builder.setModelId(modelId);
            builder.setTaskType(taskType);
            return builder.build();
        }

        private final TaskType taskType;
        private final String modelId;
        private final String input;
        private final Map<String, Object> taskSettings;

        public Request(TaskType taskType, String modelId, String input, Map<String, Object> taskSettings) {
            this.taskType = taskType;
            this.modelId = modelId;
            this.input = input;
            this.taskSettings = taskSettings;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.taskType = TaskType.fromStream(in);
            this.modelId = in.readString();
            this.input = in.readString();
            this.taskSettings = in.readMap();
        }

        public TaskType getTaskType() {
            return taskType;
        }

        public String getModelId() {
            return modelId;
        }

        public String getInput() {
            return input;
        }

        public Map<String, Object> getTaskSettings() {
            return taskSettings;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (input == null) {
                var e = new ActionRequestValidationException();
                e.addValidationError("missing input");
                return e;
            }
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            taskType.writeTo(out);
            out.writeString(modelId);
            out.writeString(input);
            out.writeGenericMap(taskSettings);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return taskType == request.taskType
                && Objects.equals(modelId, request.modelId)
                && Objects.equals(input, request.input)
                && Objects.equals(taskSettings, request.taskSettings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskType, modelId, input, taskSettings);
        }

        public static class Builder {

            private TaskType taskType;
            private String modelId;
            private String input;
            private Map<String, Object> taskSettings = Map.of();

            private Builder() {}

            public Builder setModelId(String modelId) {
                this.modelId = Objects.requireNonNull(modelId);
                return this;
            }

            public Builder setTaskType(String taskTypeStr) {
                try {
                    TaskType taskType = TaskType.fromString(taskTypeStr);
                    this.taskType = Objects.requireNonNull(taskType);
                } catch (IllegalArgumentException e) {
                    throw new ElasticsearchStatusException("Unknown task_type [{}]", RestStatus.BAD_REQUEST, taskTypeStr);
                }
                return this;
            }

            public Builder setInput(String input) {
                this.input = input;
                return this;
            }

            public Builder setTaskSettings(Map<String, Object> taskSettings) {
                this.taskSettings = taskSettings;
                return this;
            }

            public Request build() {
                return new Request(taskType, modelId, input, taskSettings);
            }
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
