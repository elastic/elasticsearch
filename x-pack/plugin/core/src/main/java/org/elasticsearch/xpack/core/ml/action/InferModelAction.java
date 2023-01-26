/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

public class InferModelAction extends ActionType<InferModelAction.Response> {
    public static final String NAME = "cluster:internal/xpack/ml/inference/infer";
    public static final String EXTERNAL_NAME = "cluster:monitor/xpack/ml/inference/infer";

    public static final InferModelAction INSTANCE = new InferModelAction(NAME);
    public static final InferModelAction EXTERNAL_INSTANCE = new InferModelAction(EXTERNAL_NAME);

    private InferModelAction(String name) {
        super(name, Response::new);
    }

    public static class Request extends ActionRequest {

        public static final ParseField MODEL_ID = new ParseField("model_id");
        public static final ParseField DOCS = new ParseField("docs");
        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static final ParseField INFERENCE_CONFIG = new ParseField("inference_config");

        static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(NAME, Builder::new);
        static {
            PARSER.declareString(Builder::setModelId, MODEL_ID);
            PARSER.declareObjectArray(Builder::setDocs, (p, c) -> p.mapOrdered(), DOCS);
            PARSER.declareString(Builder::setInferenceTimeout, TIMEOUT);
            PARSER.declareNamedObject(
                Builder::setUpdate,
                ((p, c, name) -> p.namedObject(InferenceConfigUpdate.class, name, c)),
                INFERENCE_CONFIG
            );
        }

        public static Builder parseRequest(String modelId, XContentParser parser) {
            Builder builder = PARSER.apply(parser, null);
            if (modelId != null) {
                builder.setModelId(modelId);
            }
            return builder;
        }

        public static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(10);

        private final String modelId;
        private final List<Map<String, Object>> objectsToInfer;
        private final InferenceConfigUpdate update;
        private final boolean previouslyLicensed;
        private TimeValue inferenceTimeout;
        // textInput added for uses that accept a query string
        // and do know which field the model expects to find its
        // input and so cannot construct a document.
        private final List<String> textInput;

        public static Request forDocs(
            String modelId,
            List<Map<String, Object>> docs,
            InferenceConfigUpdate update,
            boolean previouslyLicensed
        ) {
            return new Request(
                ExceptionsHelper.requireNonNull(modelId, InferModelAction.Request.MODEL_ID),
                update,
                ExceptionsHelper.requireNonNull(Collections.unmodifiableList(docs), DOCS),
                null,
                DEFAULT_TIMEOUT,
                previouslyLicensed
            );
        }

        public static Request forTextInput(String modelId, InferenceConfigUpdate update, List<String> textInput) {
            return new Request(
                modelId,
                update,
                List.of(),
                ExceptionsHelper.requireNonNull(textInput, "inference text input"),
                DEFAULT_TIMEOUT,
                false
            );
        }

        Request(
            String modelId,
            InferenceConfigUpdate inferenceConfigUpdate,
            List<Map<String, Object>> docs,
            List<String> textInput,
            TimeValue inferenceTimeout,
            boolean previouslyLicensed
        ) {
            this.modelId = ExceptionsHelper.requireNonNull(modelId, MODEL_ID);
            this.objectsToInfer = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(docs, DOCS.getPreferredName()));
            this.update = ExceptionsHelper.requireNonNull(inferenceConfigUpdate, "inference_config");
            this.textInput = textInput;
            this.previouslyLicensed = previouslyLicensed;
            this.inferenceTimeout = inferenceTimeout;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.modelId = in.readString();
            this.objectsToInfer = in.readImmutableList(StreamInput::readMap);
            this.update = in.readNamedWriteable(InferenceConfigUpdate.class);
            this.previouslyLicensed = in.readBoolean();
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_3_0)) {
                this.inferenceTimeout = in.readTimeValue();
            } else {
                this.inferenceTimeout = TimeValue.MAX_VALUE;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
                textInput = in.readOptionalStringList();
            } else {
                textInput = null;
            }
        }

        public int numberOfDocuments() {
            if (textInput != null) {
                return textInput.size();
            } else {
                return objectsToInfer.size();
            }
        }

        public String getModelId() {
            return modelId;
        }

        public List<Map<String, Object>> getObjectsToInfer() {
            return objectsToInfer;
        }

        public List<String> getTextInput() {
            return textInput;
        }

        public InferenceConfigUpdate getUpdate() {
            return update;
        }

        public boolean isPreviouslyLicensed() {
            return previouslyLicensed;
        }

        public TimeValue getInferenceTimeout() {
            return inferenceTimeout;
        }

        public void setInferenceTimeout(TimeValue inferenceTimeout) {
            this.inferenceTimeout = inferenceTimeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelId);
            out.writeCollection(objectsToInfer, StreamOutput::writeGenericMap);
            out.writeNamedWriteable(update);
            out.writeBoolean(previouslyLicensed);
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_3_0)) {
                out.writeTimeValue(inferenceTimeout);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
                out.writeOptionalStringCollection(textInput);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InferModelAction.Request that = (InferModelAction.Request) o;
            return Objects.equals(modelId, that.modelId)
                && Objects.equals(update, that.update)
                && Objects.equals(previouslyLicensed, that.previouslyLicensed)
                && Objects.equals(inferenceTimeout, that.inferenceTimeout)
                && Objects.equals(objectsToInfer, that.objectsToInfer)
                && Objects.equals(textInput, that.textInput);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, format("infer_trained_model[%s]", modelId), parentTaskId, headers);
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId, objectsToInfer, update, previouslyLicensed, inferenceTimeout, textInput);
        }

        public static class Builder {

            private String modelId;
            private List<Map<String, Object>> docs;
            private TimeValue timeout;
            private InferenceConfigUpdate update = new EmptyConfigUpdate();

            private Builder() {}

            public Builder setModelId(String modelId) {
                this.modelId = ExceptionsHelper.requireNonNull(modelId, MODEL_ID);
                return this;
            }

            public Builder setDocs(List<Map<String, Object>> docs) {
                this.docs = ExceptionsHelper.requireNonNull(docs, DOCS);
                return this;
            }

            public Builder setInferenceTimeout(TimeValue inferenceTimeout) {
                this.timeout = inferenceTimeout;
                return this;
            }

            public Builder setUpdate(InferenceConfigUpdate update) {
                this.update = update;
                return this;
            }

            public InferenceConfigUpdate getUpdate() {
                return update;
            }

            private Builder setInferenceTimeout(String inferenceTimeout) {
                return setInferenceTimeout(TimeValue.parseTimeValue(inferenceTimeout, TIMEOUT.getPreferredName()));
            }

            public Request build() {
                return new Request(modelId, update, docs, null, timeout, false);
            }
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<InferenceResults> inferenceResults;
        private final String modelId;
        private final boolean isLicensed;

        public Response(List<InferenceResults> inferenceResults, String modelId, boolean isLicensed) {
            super();
            this.inferenceResults = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(inferenceResults, "inferenceResults"));
            this.isLicensed = isLicensed;
            this.modelId = modelId;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.inferenceResults = Collections.unmodifiableList(in.readNamedWriteableList(InferenceResults.class));
            this.isLicensed = in.readBoolean();
            this.modelId = in.readOptionalString();
        }

        public List<InferenceResults> getInferenceResults() {
            return inferenceResults;
        }

        public boolean isLicensed() {
            return isLicensed;
        }

        public String getModelId() {
            return modelId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteableList(inferenceResults);
            out.writeBoolean(isLicensed);
            out.writeOptionalString(modelId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InferModelAction.Response that = (InferModelAction.Response) o;
            return isLicensed == that.isLicensed
                && Objects.equals(inferenceResults, that.inferenceResults)
                && Objects.equals(modelId, that.modelId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(inferenceResults, isLicensed, modelId);
        }

        public static Builder builder() {
            return new Builder();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("inference_results", inferenceResults.stream().map(InferenceResults::asMap).collect(Collectors.toList()));
            builder.endObject();
            return builder;
        }

        public static class Builder {
            private List<InferenceResults> inferenceResults = new ArrayList<>();
            private String modelId;
            private boolean isLicensed;

            public Builder addInferenceResults(List<InferenceResults> inferenceResults) {
                this.inferenceResults.addAll(inferenceResults);
                return this;
            }

            public Builder setLicensed(boolean licensed) {
                isLicensed = licensed;
                return this;
            }

            public Builder setModelId(String modelId) {
                this.modelId = modelId;
                return this;
            }

            public Response build() {
                return new Response(inferenceResults, modelId, isLicensed);
            }
        }

    }
}
