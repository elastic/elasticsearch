/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.DocumentExtractionRequest;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.InferenceContext;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.inference.DocumentExtractionRequest.INPUT_FIELD;
import static org.elasticsearch.inference.DocumentExtractionRequest.SUPPORTED_DOCUMENT_EXTRACTION_DATA_TYPES;
import static org.elasticsearch.inference.InferenceString.TYPE_FIELD;

public class DocumentExtractionAction extends ActionType<InferenceAction.Response> {
    public static final DocumentExtractionAction INSTANCE = new DocumentExtractionAction();
    public static final String NAME = "cluster:internal/xpack/inference/document_extraction";

    public DocumentExtractionAction() {
        super(NAME);
    }

    public static class Request extends BaseInferenceActionRequest {
        public static Request parseRequest(String inferenceEntityId, TimeValue timeout, InferenceContext context, XContentParser parser)
            throws IOException {
            var documentExtractionRequest = DocumentExtractionRequest.PARSER.apply(parser, null);
            return new Request(inferenceEntityId, documentExtractionRequest, context, timeout);
        }

        private final String inferenceEntityId;
        private final DocumentExtractionRequest documentExtractionRequest;
        private final TimeValue timeout;

        public Request(String inferenceEntityId, DocumentExtractionRequest documentExtractionRequest, @Nullable TimeValue timeout) {
            this(inferenceEntityId, documentExtractionRequest, InferenceContext.EMPTY_INSTANCE, timeout);
        }

        public Request(
            String inferenceEntityId,
            DocumentExtractionRequest documentExtractionRequest,
            InferenceContext context,
            @Nullable TimeValue timeout
        ) {
            super(context);
            this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
            this.documentExtractionRequest = Objects.requireNonNull(documentExtractionRequest);
            this.timeout = Objects.requireNonNullElse(timeout, TIMEOUT_NOT_DETERMINED);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.inferenceEntityId = in.readString();
            this.documentExtractionRequest = new DocumentExtractionRequest(in);
            this.timeout = in.readTimeValue();
        }

        public TaskType getTaskType() {
            return TaskType.DOCUMENT_EXTRACTION;
        }

        public String getInferenceEntityId() {
            return inferenceEntityId;
        }

        public DocumentExtractionRequest getDocumentExtractionRequest() {
            return documentExtractionRequest;
        }

        public boolean isStreaming() {
            // streaming is not supported for the DOCUMENT_EXTRACTION task
            return false;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException e = null;
            if (documentExtractionRequest.inputs() == null) {
                e = addValidationError(format("Field [%s] cannot be null", INPUT_FIELD), e);
            } else if (documentExtractionRequest.inputs().isEmpty()) {
                e = addValidationError(format("Field [%s] cannot be an empty array", INPUT_FIELD), e);
            } else {
                List<InferenceString> inputs = documentExtractionRequest.inputs();
                for (int i = 0; i < inputs.size(); ++i) {
                    var dataType = inputs.get(i).dataType();
                    if (SUPPORTED_DOCUMENT_EXTRACTION_DATA_TYPES.contains(dataType) == false) {
                        e = addValidationError(
                            format("Field [%s] contains unsupported [%s] value %s at index %d", INPUT_FIELD, TYPE_FIELD, dataType, i),
                            e
                        );
                    }
                }
            }

            return e;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(inferenceEntityId);
            documentExtractionRequest.writeTo(out);
            out.writeTimeValue(timeout);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return super.equals(o)
                && Objects.equals(inferenceEntityId, request.inferenceEntityId)
                && Objects.equals(documentExtractionRequest, request.documentExtractionRequest)
                && Objects.equals(timeout, request.timeout);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), inferenceEntityId, documentExtractionRequest, timeout);
        }

    }

}
