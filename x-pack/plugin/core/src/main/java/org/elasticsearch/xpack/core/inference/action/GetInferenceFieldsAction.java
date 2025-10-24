/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceResults;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GetInferenceFieldsAction extends ActionType<GetInferenceFieldsAction.Response> {
    public static final GetInferenceFieldsAction INSTANCE = new GetInferenceFieldsAction();
    public static final RemoteClusterActionType<Response> REMOTE_TYPE = new RemoteClusterActionType<>(INSTANCE.name(), Response::new);

    public static final String NAME = "cluster:monitor/xpack/inference_fields/get";

    public GetInferenceFieldsAction() {
        super(NAME);
    }

    public static class Request extends ActionRequest {
        private final List<String> indices;
        private final List<String> fields;
        private final boolean resolveWildcards;
        private final String query;

        public Request(List<String> indices, List<String> fields, boolean resolveWildcards, @Nullable String query) {
            this.indices = indices;
            this.fields = fields;
            this.resolveWildcards = resolveWildcards;
            this.query = query;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.indices = in.readCollectionAsList(StreamInput::readString);
            this.fields = in.readCollectionAsList(StreamInput::readString);
            this.resolveWildcards = in.readBoolean();
            this.query = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringCollection(indices);
            out.writeStringCollection(fields);
            out.writeBoolean(resolveWildcards);
            out.writeOptionalString(query);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public List<String> getIndices() {
            return Collections.unmodifiableList(indices);
        }

        public List<String> getFields() {
            return Collections.unmodifiableList(fields);
        }

        public boolean resolveWildcards() {
            return resolveWildcards;
        }

        public String getQuery() {
            return query;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(indices, request.indices)
                && Objects.equals(fields, request.fields)
                && resolveWildcards == request.resolveWildcards
                && Objects.equals(query, request.query);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indices, fields, resolveWildcards, query);
        }
    }

    public static class Response extends ActionResponse {
        private final Map<String, List<InferenceFieldMetadata>> inferenceFieldsMap;
        private final Map<String, InferenceResults> inferenceResultsMap;

        public Response(Map<String, List<InferenceFieldMetadata>> inferenceFieldsMap, Map<String, InferenceResults> inferenceResultsMap) {
            this.inferenceFieldsMap = inferenceFieldsMap;
            this.inferenceResultsMap = inferenceResultsMap;
        }

        public Response(StreamInput in) throws IOException {
            this.inferenceFieldsMap = in.readImmutableMap(i -> i.readCollectionAsImmutableList(InferenceFieldMetadata::new));
            this.inferenceResultsMap = in.readImmutableMap(i -> i.readNamedWriteable(InferenceResults.class));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(inferenceFieldsMap, StreamOutput::writeCollection);
            out.writeMap(inferenceResultsMap, StreamOutput::writeNamedWriteable);
        }

        public Map<String, List<InferenceFieldMetadata>> getInferenceFieldsMap() {
            return Collections.unmodifiableMap(this.inferenceFieldsMap);
        }

        public Map<String, InferenceResults> getInferenceResultsMap() {
            return Collections.unmodifiableMap(this.inferenceResultsMap);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(inferenceFieldsMap, response.inferenceFieldsMap)
                && Objects.equals(inferenceResultsMap, response.inferenceResultsMap);
        }

        @Override
        public int hashCode() {
            return Objects.hash(inferenceFieldsMap, inferenceResultsMap);
        }
    }
}
