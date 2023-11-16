/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
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
            PARSER.declareStringArray(Request.Builder::setInput, INPUT);
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
        private final List<String> input;
        private final Map<String, Object> taskSettings;

        public Request(TaskType taskType, String modelId, List<String> input, Map<String, Object> taskSettings) {
            this.taskType = taskType;
            this.modelId = modelId;
            this.input = input;
            this.taskSettings = taskSettings;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.taskType = TaskType.fromStream(in);
            this.modelId = in.readString();
            if (in.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_MULTIPLE_INPUTS)) {
                this.input = in.readStringCollectionAsList();
            } else {
                this.input = List.of(in.readString());
            }
            this.taskSettings = in.readMap();
        }

        public TaskType getTaskType() {
            return taskType;
        }

        public String getModelId() {
            return modelId;
        }

        public List<String> getInput() {
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
            if (input.isEmpty()) {
                var e = new ActionRequestValidationException();
                e.addValidationError("input array is empty");
                return e;
            }
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            taskType.writeTo(out);
            out.writeString(modelId);
            if (out.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_MULTIPLE_INPUTS)) {
                out.writeStringCollection(input);
            } else {
                out.writeString(input.get(0));
            }
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
            private List<String> input;
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

            public Builder setInput(List<String> input) {
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

        private final List<? extends InferenceResults> results;

        public Response(List<? extends InferenceResults> results) {
            this.results = results;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            if (in.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_MULTIPLE_INPUTS)) {
                results = in.readNamedWriteableCollectionAsList(InferenceResults.class);
            } else {
                results = List.of(in.readNamedWriteable(InferenceResults.class));
            }
        }

        public List<? extends InferenceResults> getResults() {
            return results;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_MULTIPLE_INPUTS)) {
                out.writeNamedWriteableCollection(results);
            } else {
                out.writeNamedWriteable(results.get(0));
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray("inference_results"); // TODO what is the name of this field?
            for (var result : results) {
                // inference results implement ToXContentFragment
                builder.startObject();
                result.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(results, response.results);
        }

        @Override
        public int hashCode() {
            return Objects.hash(results);
        }
    }
}
