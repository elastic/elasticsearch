/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

public class InferModelAction extends ActionType<InferModelAction.Response> {
    public static final String NAME = "cluster:internal/xpack/ml/inference/infer";
    public static final String EXTERNAL_NAME = "cluster:monitor/xpack/ml/inference/infer";

    public static final InferModelAction INSTANCE = new InferModelAction(NAME);
    public static final InferModelAction EXTERNAL_INSTANCE = new InferModelAction(EXTERNAL_NAME);

    private InferModelAction(String name) {
        super(name);
    }

    public static class Request extends LegacyActionRequest {

        public static final ParseField ID = new ParseField("id");
        public static final ParseField DEPLOYMENT_ID = new ParseField("deployment_id");
        public static final ParseField DOCS = new ParseField("docs");
        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static final ParseField INFERENCE_CONFIG = new ParseField("inference_config");

        static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(NAME, Builder::new);

        static {
            PARSER.declareString(Builder::setId, ID);
            PARSER.declareObjectArray(Builder::setDocs, (p, c) -> p.mapOrdered(), DOCS);
            PARSER.declareString(Builder::setInferenceTimeout, TIMEOUT);
            PARSER.declareNamedObject(
                Builder::setUpdate,
                ((p, c, name) -> p.namedObject(InferenceConfigUpdate.class, name, c)),
                INFERENCE_CONFIG
            );
        }

        public static Builder parseRequest(String id, XContentParser parser) {
            Builder builder = PARSER.apply(parser, null);
            if (id != null) {
                builder.setId(id);
            }
            return builder;
        }

        public static final TimeValue DEFAULT_TIMEOUT_FOR_API = TimeValue.timeValueSeconds(10);
        public static final TimeValue DEFAULT_TIMEOUT_FOR_INGEST = TimeValue.MAX_VALUE;

        private final String id;
        private final List<Map<String, Object>> objectsToInfer;
        private final InferenceConfigUpdate update;
        private final boolean previouslyLicensed;
        private final TimeValue inferenceTimeout;
        // textInput added for uses that accept a query string
        // and do know which field the model expects to find its
        // input and so cannot construct a document.
        private final List<String> textInput;
        private boolean highPriority;
        private TrainedModelPrefixStrings.PrefixType prefixType = TrainedModelPrefixStrings.PrefixType.NONE;
        private boolean chunked = false;

        /**
         * Build a request from a list of documents as maps.
         *
         * @param id                 The model Id
         * @param docs               List of document maps
         * @param update             Inference config update
         * @param previouslyLicensed License has been checked previously
         *                           and can now be skipped
         * @param inferenceTimeout   The inference timeout (how long the
         *                           request waits in the inference queue for)
         * @return the new Request
         */
        public static Request forIngestDocs(
            String id,
            List<Map<String, Object>> docs,
            InferenceConfigUpdate update,
            boolean previouslyLicensed,
            TimeValue inferenceTimeout
        ) {
            return new Request(
                ExceptionsHelper.requireNonNull(id, Request.ID),
                update,
                ExceptionsHelper.requireNonNull(Collections.unmodifiableList(docs), DOCS),
                null,
                inferenceTimeout,
                previouslyLicensed
            );
        }

        /**
         * Build a request from a list of strings, each string
         * is one evaluation of the model.
         *
         * @param id                 The model Id
         * @param update             Inference config update
         * @param textInput          Inference input
         * @param previouslyLicensed License has been checked previously
         *                           and can now be skipped
         * @param inferenceTimeout   The inference timeout (how long the
         *                           request waits in the inference queue for)
         * @return the new Request
         */
        public static Request forTextInput(
            String id,
            InferenceConfigUpdate update,
            List<String> textInput,
            boolean previouslyLicensed,
            TimeValue inferenceTimeout
        ) {
            return new Request(
                id,
                update,
                List.of(),
                ExceptionsHelper.requireNonNull(textInput, "inference text input"),
                inferenceTimeout,
                previouslyLicensed
            );
        }

        Request(
            String id,
            InferenceConfigUpdate inferenceConfigUpdate,
            List<Map<String, Object>> docs,
            List<String> textInput,
            TimeValue inferenceTimeout,
            boolean previouslyLicensed
        ) {
            this.id = ExceptionsHelper.requireNonNull(id, ID);
            this.objectsToInfer = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(docs, DOCS.getPreferredName()));
            this.update = ExceptionsHelper.requireNonNull(inferenceConfigUpdate, "inference_config");
            this.textInput = textInput;
            this.previouslyLicensed = previouslyLicensed;
            this.inferenceTimeout = inferenceTimeout;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.id = in.readString();
            this.objectsToInfer = in.readCollectionAsImmutableList(StreamInput::readGenericMap);
            this.update = in.readNamedWriteable(InferenceConfigUpdate.class);
            this.previouslyLicensed = in.readBoolean();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_3_0)) {
                this.inferenceTimeout = in.readTimeValue();
            } else {
                this.inferenceTimeout = TimeValue.MAX_VALUE;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_7_0)) {
                textInput = in.readOptionalStringCollectionAsList();
            } else {
                textInput = null;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
                highPriority = in.readBoolean();
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                prefixType = in.readEnum(TrainedModelPrefixStrings.PrefixType.class);
            } else {
                prefixType = TrainedModelPrefixStrings.PrefixType.NONE;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                chunked = in.readBoolean();
            } else {
                chunked = false;
            }
        }

        public int numberOfDocuments() {
            if (textInput != null) {
                return textInput.size();
            } else {
                return objectsToInfer.size();
            }
        }

        public String getId() {
            return id;
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

        public boolean isHighPriority() {
            return highPriority;
        }

        public void setHighPriority(boolean highPriority) {
            this.highPriority = highPriority;
        }

        public void setPrefixType(TrainedModelPrefixStrings.PrefixType prefixType) {
            this.prefixType = prefixType;
        }

        public TrainedModelPrefixStrings.PrefixType getPrefixType() {
            return prefixType;
        }

        public void setChunked(boolean chunked) {
            this.chunked = chunked;
        }

        public boolean isChunked() {
            return chunked;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            out.writeCollection(objectsToInfer, StreamOutput::writeGenericMap);
            out.writeNamedWriteable(update);
            out.writeBoolean(previouslyLicensed);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_3_0)) {
                out.writeTimeValue(inferenceTimeout);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_7_0)) {
                out.writeOptionalStringCollection(textInput);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
                out.writeBoolean(highPriority);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                out.writeEnum(prefixType);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                out.writeBoolean(chunked);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request that = (Request) o;
            return Objects.equals(id, that.id)
                && Objects.equals(update, that.update)
                && Objects.equals(previouslyLicensed, that.previouslyLicensed)
                && Objects.equals(inferenceTimeout, that.inferenceTimeout)
                && Objects.equals(objectsToInfer, that.objectsToInfer)
                && Objects.equals(textInput, that.textInput)
                && (highPriority == that.highPriority)
                && (prefixType == that.prefixType)
                && (chunked == that.chunked);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, format("infer_trained_model[%s]", this.id), parentTaskId, headers);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                id,
                objectsToInfer,
                update,
                previouslyLicensed,
                inferenceTimeout,
                textInput,
                highPriority,
                prefixType,
                chunked
            );
        }

        public static class Builder {

            private String id;
            private List<Map<String, Object>> docs;
            private TimeValue timeout;
            private InferenceConfigUpdate update = new EmptyConfigUpdate();

            private Builder() {}

            public Builder setId(String id) {
                this.id = ExceptionsHelper.requireNonNull(id, ID);
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
                return new Request(id, update, docs, null, timeout, false);
            }
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<InferenceResults> inferenceResults;
        private final String id;
        private final boolean isLicensed;

        public Response(List<InferenceResults> inferenceResults, String id, boolean isLicensed) {
            super();
            this.inferenceResults = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(inferenceResults, "inferenceResults"));
            this.isLicensed = isLicensed;
            this.id = id;
        }

        public Response(StreamInput in) throws IOException {
            this.inferenceResults = Collections.unmodifiableList(in.readNamedWriteableCollectionAsList(InferenceResults.class));
            this.isLicensed = in.readBoolean();
            this.id = in.readOptionalString();
        }

        public List<InferenceResults> getInferenceResults() {
            return inferenceResults;
        }

        public boolean isLicensed() {
            return isLicensed;
        }

        public String getId() {
            return id;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteableCollection(inferenceResults);
            out.writeBoolean(isLicensed);
            out.writeOptionalString(id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return isLicensed == that.isLicensed && Objects.equals(inferenceResults, that.inferenceResults) && Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(inferenceResults, isLicensed, id);
        }

        public static Builder builder() {
            return new Builder();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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

        public static class Builder {
            private final List<InferenceResults> inferenceResults = new ArrayList<>();
            private String id;
            private boolean isLicensed;

            public Builder addInferenceResults(List<InferenceResults> inferenceResults) {
                this.inferenceResults.addAll(inferenceResults);
                return this;
            }

            public Builder setLicensed(boolean licensed) {
                isLicensed = licensed;
                return this;
            }

            public Builder setId(String id) {
                this.id = id;
                return this;
            }

            public Response build() {
                return new Response(inferenceResults, id, isLicensed);
            }
        }

    }
}
