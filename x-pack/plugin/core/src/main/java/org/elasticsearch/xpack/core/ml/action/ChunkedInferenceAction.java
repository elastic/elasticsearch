/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ChunkedInferenceAction extends ActionType<ChunkedInferenceAction.Response> {

    public static final ChunkedInferenceAction INSTANCE = new ChunkedInferenceAction();
    public static final String NAME = "cluster:internal/xpack/ml/chunkedinference";

    static final ObjectParser<Request.Builder, Void> PARSER = new ObjectParser<>(NAME, ChunkedInferenceAction.Request.Builder::new);
    static {
        PARSER.declareStringArray(Request.Builder::setInputs, new ParseField("inputs"));
        PARSER.declareInt(Request.Builder::setWindowSize, new ParseField("window_size"));
        PARSER.declareInt(Request.Builder::setSpan, new ParseField("span"));
    }

    public static Request parseRequest(String id, TimeValue timeout, XContentParser parser) {
        Request.Builder builder = PARSER.apply(parser, null);
        if (id != null) {
            builder.setId(id);
        }
        if (timeout != null) {
            builder.setTimeout(timeout);
        }
        return builder.build();
    }

    public ChunkedInferenceAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {
        public static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(10);

        private final String modelId;
        private final List<String> inputs;
        private final Integer windowSize;
        private final Integer span;
        private final TimeValue timeout;

        public Request(String modelId, List<String> inputs, @Nullable Integer windowSize, @Nullable Integer span, TimeValue timeout) {
            this.modelId = modelId;
            this.inputs = inputs;
            this.windowSize = windowSize;
            this.span = span;
            this.timeout = timeout;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.modelId = in.readString();
            this.inputs = in.readStringCollectionAsList();
            this.windowSize = in.readOptionalVInt();
            this.span = in.readOptionalVInt();
            this.timeout = in.readTimeValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelId);
            out.writeStringCollection(inputs);
            out.writeOptionalVInt(windowSize);
            out.writeOptionalVInt(span);
            out.writeTimeValue(timeout);
        }

        public boolean containsWindowOptions() {
            return span != null || windowSize != null;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (windowSize == null || span == null) {
                // need both fields for validation
                return null;
            }

            var failedValidation = new ActionRequestValidationException();

            if (span <= 0 || windowSize <= 0) {
                failedValidation.addValidationError("window size and overlap must both be greater than 0");
            }
            if (span >= windowSize) {
                failedValidation.addValidationError("span must be less than window size");
            }

            return failedValidation.validationErrors().isEmpty() ? null : failedValidation;
        }

        public String getModelId() {
            return modelId;
        }

        public Integer getSpan() {
            return span;
        }

        public Integer getWindowSize() {
            return windowSize;
        }

        public List<String> getInputs() {
            return inputs;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(span, request.span)
                && Objects.equals(modelId, request.modelId)
                && Objects.equals(inputs, request.inputs)
                && Objects.equals(windowSize, request.windowSize)
                && Objects.equals(timeout, request.timeout);
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId, windowSize, span, inputs, timeout);
        }

        public static class Builder {

            private String id;
            private List<String> inputs;
            private Integer span = null;
            private Integer windowSize = null;
            private TimeValue timeout = DEFAULT_TIMEOUT;

            public Builder setId(String id) {
                this.id = id;
                return this;
            }

            public Builder setInputs(List<String> inputs) {
                this.inputs = inputs;
                return this;
            }

            public Builder setWindowSize(int windowSize) {
                this.windowSize = windowSize;
                return this;
            }

            public Builder setSpan(int span) {
                this.span = span;
                return this;
            }

            public Builder setTimeout(TimeValue timeout) {
                this.timeout = timeout;
                return this;
            }

            public Request build() {
                return new Request(id, inputs, windowSize, span, timeout);
            }
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<InferenceResults> inferenceResults;

        public Response(List<InferenceResults> inferenceResults) {
            super();
            this.inferenceResults = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(inferenceResults, "inferenceResults"));
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.inferenceResults = Collections.unmodifiableList(in.readNamedWriteableCollectionAsList(InferenceResults.class));
        }

        public List<InferenceResults> getInferenceResults() {
            return inferenceResults;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteableCollection(inferenceResults);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject();
            builder.startArray("inference_results");
            for (var inference : inferenceResults) {
                // inference results implement ToXContentFragment
                builder.startObject();
                inference.toXContent(builder, params);
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
            Response that = (Response) o;
            return Objects.equals(inferenceResults, that.inferenceResults);
        }

        @Override
        public int hashCode() {
            return Objects.hash(inferenceResults);
        }
    }
}
