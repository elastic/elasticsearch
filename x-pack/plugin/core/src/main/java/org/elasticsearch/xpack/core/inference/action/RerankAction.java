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
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.RerankRequest;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.InferenceContext;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.inference.InferenceString.TYPE_FIELD;
import static org.elasticsearch.inference.RerankRequest.INPUT_FIELD;
import static org.elasticsearch.inference.RerankRequest.QUERY_FIELD;
import static org.elasticsearch.inference.RerankRequest.SUPPORTED_RERANK_DATA_TYPES;
import static org.elasticsearch.inference.RerankRequest.TOP_N_FIELD;

public class RerankAction extends ActionType<InferenceAction.Response> {
    public static final RerankAction INSTANCE = new RerankAction();
    public static final String NAME = "cluster:internal/xpack/inference/rerank";

    public RerankAction() {
        super(NAME);
    }

    public static class Request extends BaseInferenceActionRequest {
        public static Request parseRequest(String inferenceEntityId, TimeValue timeout, InferenceContext context, XContentParser parser)
            throws IOException {
            var rerankRequest = RerankRequest.PARSER.apply(parser, null);
            return new Request(inferenceEntityId, rerankRequest, context, timeout);
        }

        private final String inferenceEntityId;
        private final RerankRequest rerankRequest;
        private final TimeValue timeout;

        public Request(String inferenceEntityId, RerankRequest rerankRequest, @Nullable TimeValue timeout) {
            this(inferenceEntityId, rerankRequest, InferenceContext.EMPTY_INSTANCE, timeout);
        }

        public Request(String inferenceEntityId, RerankRequest rerankRequest, InferenceContext context, @Nullable TimeValue timeout) {
            super(context);
            this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
            this.rerankRequest = Objects.requireNonNull(rerankRequest);
            this.timeout = Objects.requireNonNullElse(timeout, TIMEOUT_NOT_DETERMINED);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.inferenceEntityId = in.readString();
            this.rerankRequest = new RerankRequest(in);
            this.timeout = in.readTimeValue();
        }

        public TaskType getTaskType() {
            return TaskType.RERANK;
        }

        public String getInferenceEntityId() {
            return inferenceEntityId;
        }

        public RerankRequest getRerankRequest() {
            return rerankRequest;
        }

        public boolean isStreaming() {
            // streaming is not supported for the RERANK task
            return false;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException e = null;
            if (rerankRequest.inputs() == null) {
                e = addValidationError(Strings.format("Field [%s] cannot be null", INPUT_FIELD), e);
            } else if (rerankRequest.inputs().isEmpty()) {
                e = addValidationError(Strings.format("Field [%s] cannot be an empty array", INPUT_FIELD), e);
            } else {
                List<InferenceString> inputs = rerankRequest.inputs();
                for (int i = 0; i < inputs.size(); ++i) {
                    var dataType = inputs.get(i).dataType();
                    if (SUPPORTED_RERANK_DATA_TYPES.contains(dataType) == false) {
                        e = addValidationError(
                            format("Field [%s] contains unsupported [%s] value %s at index %d", INPUT_FIELD, TYPE_FIELD, dataType, i),
                            e
                        );
                    }
                }
            }

            if (rerankRequest.query() == null) {
                e = addValidationError(format("Field [%s] cannot be null", QUERY_FIELD), e);
            } else if (rerankRequest.query().value().isEmpty()) {
                e = addValidationError(format("Field [%s] cannot be empty", QUERY_FIELD), e);
            } else if (SUPPORTED_RERANK_DATA_TYPES.contains(rerankRequest.query().dataType()) == false) {
                e = addValidationError(
                    format("Field [%s] contains unsupported [%s] value %s", QUERY_FIELD, TYPE_FIELD, rerankRequest.query().dataType()),
                    e
                );
            }

            if (rerankRequest.topN() != null && rerankRequest.topN() < 1) {
                e = addValidationError(format("Field [%s] must be greater than or equal to 1", TOP_N_FIELD), e);
            }
            return e;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(inferenceEntityId);
            rerankRequest.writeTo(out);
            out.writeTimeValue(timeout);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return super.equals(o)
                && Objects.equals(inferenceEntityId, request.inferenceEntityId)
                && Objects.equals(rerankRequest, request.rerankRequest)
                && Objects.equals(timeout, request.timeout);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), inferenceEntityId, rerankRequest, timeout);
        }

    }

}
