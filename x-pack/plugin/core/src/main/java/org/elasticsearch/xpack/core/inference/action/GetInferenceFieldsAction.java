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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceResults;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetInferenceFieldsAction extends ActionType<GetInferenceFieldsAction.Response> {
    public static final GetInferenceFieldsAction INSTANCE = new GetInferenceFieldsAction();
    public static final RemoteClusterActionType<Response> REMOTE_TYPE = new RemoteClusterActionType<>(INSTANCE.name(), Response::new);

    public static final String NAME = "cluster:internal/xpack/inference/fields/get";

    public GetInferenceFieldsAction() {
        super(NAME);
    }

    public static class Request extends ActionRequest {
        private final Set<String> indices;
        private final Map<String, Float> fields;
        private final boolean resolveWildcards;
        private final boolean useDefaultFields;
        private final String query;
        private final IndicesOptions indicesOptions;

        public Request(
            Set<String> indices,
            Map<String, Float> fields,
            boolean resolveWildcards,
            boolean useDefaultFields,
            @Nullable String query
        ) {
            this(indices, fields, resolveWildcards, useDefaultFields, query, null);
        }

        public Request(
            Set<String> indices,
            Map<String, Float> fields,
            boolean resolveWildcards,
            boolean useDefaultFields,
            @Nullable String query,
            @Nullable IndicesOptions indicesOptions
        ) {
            this.indices = indices;
            this.fields = fields;
            this.resolveWildcards = resolveWildcards;
            this.useDefaultFields = useDefaultFields;
            this.query = query;
            this.indicesOptions = indicesOptions == null ? IndicesOptions.DEFAULT : indicesOptions;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.indices = in.readCollectionAsSet(StreamInput::readString);
            this.fields = in.readMap(StreamInput::readFloat);
            this.resolveWildcards = in.readBoolean();
            this.useDefaultFields = in.readBoolean();
            this.query = in.readOptionalString();
            this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringCollection(indices);
            out.writeMap(fields, StreamOutput::writeFloat);
            out.writeBoolean(resolveWildcards);
            out.writeBoolean(useDefaultFields);
            out.writeOptionalString(query);
            indicesOptions.writeIndicesOptions(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (indices == null) {
                validationException = addValidationError("indices must not be null", validationException);
            }

            if (fields == null) {
                validationException = addValidationError("fields must not be null", validationException);
            } else {
                for (var entry : fields.entrySet()) {
                    if (entry.getValue() == null) {
                        validationException = addValidationError(
                            "weight for field [" + entry.getKey() + "] must not be null",
                            validationException
                        );
                    }
                }
            }

            return validationException;
        }

        public Set<String> getIndices() {
            return Collections.unmodifiableSet(indices);
        }

        public Map<String, Float> getFields() {
            return Collections.unmodifiableMap(fields);
        }

        public boolean resolveWildcards() {
            return resolveWildcards;
        }

        public boolean useDefaultFields() {
            return useDefaultFields;
        }

        public String getQuery() {
            return query;
        }

        public IndicesOptions getIndicesOptions() {
            return indicesOptions;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(indices, request.indices)
                && Objects.equals(fields, request.fields)
                && resolveWildcards == request.resolveWildcards
                && useDefaultFields == request.useDefaultFields
                && Objects.equals(query, request.query)
                && Objects.equals(indicesOptions, request.indicesOptions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indices, fields, resolveWildcards, useDefaultFields, query, indicesOptions);
        }
    }

    public static class Response extends ActionResponse {
        private final Map<String, List<ExtendedInferenceFieldMetadata>> inferenceFieldsMap;
        private final Map<String, InferenceResults> inferenceResultsMap;

        public Response(
            Map<String, List<ExtendedInferenceFieldMetadata>> inferenceFieldsMap,
            Map<String, InferenceResults> inferenceResultsMap
        ) {
            this.inferenceFieldsMap = inferenceFieldsMap;
            this.inferenceResultsMap = inferenceResultsMap;
        }

        public Response(StreamInput in) throws IOException {
            this.inferenceFieldsMap = in.readImmutableMap(i -> i.readCollectionAsImmutableList(ExtendedInferenceFieldMetadata::new));
            this.inferenceResultsMap = in.readImmutableMap(i -> i.readNamedWriteable(InferenceResults.class));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(inferenceFieldsMap, StreamOutput::writeCollection);
            out.writeMap(inferenceResultsMap, StreamOutput::writeNamedWriteable);
        }

        public Map<String, List<ExtendedInferenceFieldMetadata>> getInferenceFieldsMap() {
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

    public record ExtendedInferenceFieldMetadata(InferenceFieldMetadata inferenceFieldMetadata, float weight) implements Writeable {
        public ExtendedInferenceFieldMetadata(StreamInput in) throws IOException {
            this(new InferenceFieldMetadata(in), in.readFloat());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeWriteable(inferenceFieldMetadata);
            out.writeFloat(weight);
        }
    }
}
