/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.aiindex.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.contextengine.aiindex.AiIndex;
import org.elasticsearch.xpack.contextengine.aiindex.TypeValue;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutAiIndexAction {

    public static final String NAME = "cluster:admin/xpack/context_engine/ai_index/put";
    public static final ActionType<Response> INSTANCE = new ActionType<>(NAME);

    private PutAiIndexAction() {/* no instances */}

    /**
     * Creates or replaces an AI index record. The id comes from the URL and the space from the body; with {@code create}
     * the request fails when the record already exists.
     */
    public static class Request extends UntypedActionRequest implements ToXContentObject {

        private final String id;
        private final AiIndex aiIndex;
        private final boolean create;

        public Request(String id, AiIndex aiIndex, boolean create) {
            this.id = id;
            this.aiIndex = aiIndex;
            this.create = create;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.id = in.readString();
            this.aiIndex = new AiIndex(in);
            this.create = in.readBoolean();
        }

        public static Request fromXContent(String id, boolean create, XContentParser parser) throws IOException {
            return new Request(id, AiIndex.fromXContent(parser), create);
        }

        public String id() {
            return id;
        }

        public AiIndex aiIndex() {
            return aiIndex;
        }

        public boolean create() {
            return create;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            aiIndex.writeTo(out);
            out.writeBoolean(create);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return aiIndex.toXContent(builder, params);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.isNullOrEmpty(id)) {
                validationException = addValidationError("[id] cannot be null or empty", validationException);
            } else if (id.contains(":")) {
                validationException = addValidationError("[id] cannot contain [:]", validationException);
            }
            if (aiIndex.id() != null && aiIndex.id().equals(id) == false) {
                validationException = addValidationError("[id] in the body must match the id in the URL", validationException);
            }
            if (Strings.isNullOrEmpty(aiIndex.space())) {
                validationException = addValidationError("[space] cannot be null or empty", validationException);
            } else if (aiIndex.space().contains(":")) {
                validationException = addValidationError("[space] cannot contain [:]", validationException);
            }
            if (aiIndex.dest() == null) {
                validationException = addValidationError("[dest] is required", validationException);
            } else {
                if (AiIndex.DEST_TYPE_INDEX.equals(aiIndex.dest().type()) == false
                    && AiIndex.DEST_TYPE_DATA_STREAM.equals(aiIndex.dest().type()) == false) {
                    validationException = addValidationError(
                        "[dest.type] must be one of [" + AiIndex.DEST_TYPE_INDEX + ", " + AiIndex.DEST_TYPE_DATA_STREAM + "]",
                        validationException
                    );
                }
                if (Strings.isNullOrEmpty(aiIndex.dest().value())) {
                    validationException = addValidationError("[dest.value] cannot be null or empty", validationException);
                }
            }
            validationException = validateTypeValues(
                AiIndex.AUTOMATIONS_FIELD.getPreferredName(),
                aiIndex.automations(),
                validationException
            );
            validationException = validateTypeValues(AiIndex.SOURCES_FIELD.getPreferredName(), aiIndex.sources(), validationException);
            validationException = validateTypeValues(AiIndex.TRACES_FIELD.getPreferredName(), aiIndex.traces(), validationException);
            if (aiIndex.dateCreated() != null || aiIndex.dateModified() != null) {
                validationException = addValidationError(
                    "[date_created] and [date_modified] are set at write time and cannot be provided",
                    validationException
                );
            }
            return validationException;
        }

        private static ActionRequestValidationException validateTypeValues(
            String field,
            List<TypeValue> values,
            ActionRequestValidationException validationException
        ) {
            for (TypeValue value : values) {
                if (Strings.isNullOrEmpty(value.type()) || Strings.isNullOrEmpty(value.value())) {
                    return addValidationError("[" + field + "] items must have a non-empty [type] and [value]", validationException);
                }
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return create == request.create && Objects.equals(id, request.id) && Objects.equals(aiIndex, request.aiIndex);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, aiIndex, create);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final AiIndex aiIndex;
        private final DocWriteResponse.Result result;

        public Response(AiIndex aiIndex, DocWriteResponse.Result result) {
            this.aiIndex = aiIndex;
            this.result = result;
        }

        public Response(StreamInput in) throws IOException {
            this.aiIndex = new AiIndex(in);
            this.result = DocWriteResponse.Result.readFrom(in);
        }

        public AiIndex aiIndex() {
            return aiIndex;
        }

        public DocWriteResponse.Result result() {
            return result;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            aiIndex.writeTo(out);
            result.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("result", result.getLowercase());
            builder.field("ai_index", aiIndex);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(aiIndex, response.aiIndex) && result == response.result;
        }

        @Override
        public int hashCode() {
            return Objects.hash(aiIndex, result);
        }
    }
}
