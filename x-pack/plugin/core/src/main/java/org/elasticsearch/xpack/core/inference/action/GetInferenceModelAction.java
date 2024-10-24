/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GetInferenceModelAction extends ActionType<GetInferenceModelAction.Response> {

    public static final GetInferenceModelAction INSTANCE = new GetInferenceModelAction();
    public static final String NAME = "cluster:monitor/xpack/inference/get";

    public GetInferenceModelAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<GetInferenceModelAction.Request> {

        private static boolean PERSIST_DEFAULT_CONFIGS = true;

        private final String inferenceEntityId;
        private final TaskType taskType;
        // Default endpoint configurations are persisted on first read.
        // Set to false to avoid persisting on read.
        // This setting only applies to GET * requests. It has
        // no effect when getting a single model
        private final boolean persistDefaultConfig;

        public Request(String inferenceEntityId, TaskType taskType) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
            this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
            this.taskType = Objects.requireNonNull(taskType);
            this.persistDefaultConfig = PERSIST_DEFAULT_CONFIGS;
        }

        public Request(String inferenceEntityId, TaskType taskType, boolean persistDefaultConfig) {
            super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
            this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
            this.taskType = Objects.requireNonNull(taskType);
            this.persistDefaultConfig = persistDefaultConfig;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.inferenceEntityId = in.readString();
            this.taskType = TaskType.fromStream(in);
            if (in.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_DONT_PERSIST_ON_READ)
                || in.getTransportVersion().isPatchFrom(TransportVersions.INFERENCE_DONT_PERSIST_ON_READ_BACKPORT_8_16)) {
                this.persistDefaultConfig = in.readBoolean();
            } else {
                this.persistDefaultConfig = PERSIST_DEFAULT_CONFIGS;
            }

        }

        public String getInferenceEntityId() {
            return inferenceEntityId;
        }

        public TaskType getTaskType() {
            return taskType;
        }

        public boolean isPersistDefaultConfig() {
            return persistDefaultConfig;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(inferenceEntityId);
            taskType.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_DONT_PERSIST_ON_READ)
                || out.getTransportVersion().isPatchFrom(TransportVersions.INFERENCE_DONT_PERSIST_ON_READ_BACKPORT_8_16)) {
                out.writeBoolean(this.persistDefaultConfig);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(inferenceEntityId, request.inferenceEntityId)
                && taskType == request.taskType
                && persistDefaultConfig == request.persistDefaultConfig;
        }

        @Override
        public int hashCode() {
            return Objects.hash(inferenceEntityId, taskType, persistDefaultConfig);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<ModelConfigurations> endpoints;

        public Response(List<ModelConfigurations> endpoints) {
            this.endpoints = endpoints;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                endpoints = in.readCollectionAsList(ModelConfigurations::new);
            } else {
                endpoints = new ArrayList<>();
                endpoints.add(new ModelConfigurations(in));
            }
        }

        public List<ModelConfigurations> getEndpoints() {
            return endpoints;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                out.writeCollection(endpoints);
            } else {
                endpoints.get(0).writeTo(out);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray("endpoints");
            for (var endpoint : endpoints) {
                if (endpoint != null) {
                    endpoint.toFilteredXContent(builder, params);
                }
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GetInferenceModelAction.Response response = (GetInferenceModelAction.Response) o;
            return Objects.equals(endpoints, response.endpoints);
        }

        @Override
        public int hashCode() {
            return Objects.hash(endpoints);
        }
    }
}
