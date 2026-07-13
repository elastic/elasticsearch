/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class GetInferenceServicesAction extends ActionType<GetInferenceServicesAction.Response> {

    public static final GetInferenceServicesAction INSTANCE = new GetInferenceServicesAction();
    public static final String NAME = "cluster:monitor/xpack/inference/services/get";

    public GetInferenceServicesAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<GetInferenceServicesAction.Request> {

        private final TaskType taskType;

        public Request(TaskType taskType) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
            this.taskType = Objects.requireNonNull(taskType);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.taskType = TaskType.fromStream(in);
        }

        public TaskType getTaskType() {
            return taskType;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            taskType.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return taskType == request.taskType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskType);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<InferenceServiceConfiguration> configurations;

        public Response(List<InferenceServiceConfiguration> configurations) {
            this.configurations = configurations;
        }

        public List<InferenceServiceConfiguration> getConfigurations() {
            return configurations;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(configurations);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray();
            for (var configuration : configurations) {
                if (configuration != null) {
                    configuration.toXContent(builder, params);
                }
            }
            builder.endArray();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GetInferenceServicesAction.Response response = (GetInferenceServicesAction.Response) o;
            return Objects.equals(configurations, response.configurations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(configurations);
        }
    }
}
