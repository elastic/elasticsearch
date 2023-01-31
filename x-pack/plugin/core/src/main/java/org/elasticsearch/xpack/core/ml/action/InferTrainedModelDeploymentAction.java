/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.tasks.BaseTasksRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.core.Strings.format;

public class InferTrainedModelDeploymentAction extends ActionType<InferTrainedModelDeploymentAction.Response> {

    public static final InferTrainedModelDeploymentAction INSTANCE = new InferTrainedModelDeploymentAction();

    /**
     * Do not call this action directly, use InferModelAction instead
     * which will perform various checks and set the node the request
     * should execute on.
     *
     * The action is poorly named as once it was publicly accessible
     * and exposed through a REST API now it _must_ only called internally.
     */
    public static final String NAME = "cluster:monitor/xpack/ml/trained_models/deployment/infer";

    public InferTrainedModelDeploymentAction() {
        super(NAME, InferTrainedModelDeploymentAction.Response::new);
    }

    /**
     * Request for inference against the deployment.
     *
     * The task gets routed to a node that indicates its local model assignment is started
     *
     * For indicating timeout, the caller should call `setInferenceTimeout` and not the base class `setTimeout` method
     */
    public static class Request extends BaseTasksRequest<Request> {

        public static final ParseField DOCS = new ParseField("docs");
        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static final ParseField INFERENCE_CONFIG = new ParseField("inference_config");

        public static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(10);

        static final ObjectParser<Request.Builder, Void> PARSER = new ObjectParser<>(NAME, Request.Builder::new);
        static {
            PARSER.declareString(Request.Builder::setModelId, InferModelAction.Request.MODEL_ID);
            PARSER.declareObjectArray(Request.Builder::setDocs, (p, c) -> p.mapOrdered(), DOCS);
            PARSER.declareString(Request.Builder::setInferenceTimeout, TIMEOUT);
            PARSER.declareNamedObject(
                Request.Builder::setUpdate,
                ((p, c, name) -> p.namedObject(InferenceConfigUpdate.class, name, c)),
                INFERENCE_CONFIG
            );
        }

        public static Request.Builder parseRequest(String modelId, XContentParser parser) {
            Request.Builder builder = PARSER.apply(parser, null);
            if (modelId != null) {
                builder.setModelId(modelId);
            }
            return builder;
        }

        private String modelId;
        private final List<Map<String, Object>> docs;
        private final InferenceConfigUpdate update;
        private final TimeValue inferenceTimeout;
        private boolean skipQueue = false;
        // textInput added for uses that accept a query string
        // and do know which field the model expects to find its
        // input and so cannot construct a document.
        private final List<String> textInput;

        public static Request forDocs(
            String modelId,
            InferenceConfigUpdate update,
            List<Map<String, Object>> docs,
            TimeValue inferenceTimeout
        ) {
            return new Request(
                ExceptionsHelper.requireNonNull(modelId, InferModelAction.Request.MODEL_ID),
                update,
                ExceptionsHelper.requireNonNull(Collections.unmodifiableList(docs), DOCS),
                null,
                false,
                inferenceTimeout
            );
        }

        public static Request forTextInput(
            String modelId,
            InferenceConfigUpdate update,
            List<String> textInput,
            TimeValue inferenceTimeout
        ) {
            return new Request(
                ExceptionsHelper.requireNonNull(modelId, InferModelAction.Request.MODEL_ID),
                update,
                List.of(),
                ExceptionsHelper.requireNonNull(textInput, "inference text input"),
                false,
                inferenceTimeout
            );
        }

        // for tests
        Request(
            String modelId,
            InferenceConfigUpdate update,
            List<Map<String, Object>> docs,
            List<String> textInput,
            boolean skipQueue,
            TimeValue inferenceTimeout
        ) {
            this.modelId = ExceptionsHelper.requireNonNull(modelId, InferModelAction.Request.MODEL_ID);
            this.docs = docs;
            this.textInput = textInput;
            this.update = update;
            this.inferenceTimeout = inferenceTimeout;
            this.skipQueue = skipQueue;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            modelId = in.readString();
            docs = in.readImmutableList(StreamInput::readMap);
            update = in.readOptionalNamedWriteable(InferenceConfigUpdate.class);
            inferenceTimeout = in.readOptionalTimeValue();
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_3_0)) {
                skipQueue = in.readBoolean();
            }
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
                textInput = in.readOptionalStringList();
            } else {
                textInput = null;
            }
        }

        public String getModelId() {
            return modelId;
        }

        public List<Map<String, Object>> getDocs() {
            return docs;
        }

        public List<String> getTextInput() {
            return textInput;
        }

        public InferenceConfigUpdate getUpdate() {
            return Optional.ofNullable(update).orElse(new EmptyConfigUpdate());
        }

        public TimeValue getInferenceTimeout() {
            return inferenceTimeout == null ? DEFAULT_TIMEOUT : inferenceTimeout;
        }

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        /**
         * This is always null as we want the inference call to handle the timeout, not the tasks framework
         * @return null
         */
        @Override
        @Nullable
        public TimeValue getTimeout() {
            return null;
        }

        public void setSkipQueue(boolean skipQueue) {
            this.skipQueue = skipQueue;
        }

        public boolean isSkipQueue() {
            return skipQueue;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = super.validate();
            if (docs == null) {
                validationException = addValidationError("[" + DOCS.getPreferredName() + "] must not be null", validationException);
            } else {
                if (docs.isEmpty() && textInput == null) {
                    validationException = addValidationError("at least one document is required ", validationException);
                }
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelId);
            out.writeCollection(docs, StreamOutput::writeGenericMap);
            out.writeOptionalNamedWriteable(update);
            out.writeOptionalTimeValue(inferenceTimeout);
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_3_0)) {
                out.writeBoolean(skipQueue);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
                out.writeOptionalStringCollection(textInput);
            }
        }

        @Override
        public boolean match(Task task) {
            return StartTrainedModelDeploymentAction.TaskMatcher.match(task, modelId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InferTrainedModelDeploymentAction.Request that = (InferTrainedModelDeploymentAction.Request) o;
            return Objects.equals(modelId, that.modelId)
                && Objects.equals(docs, that.docs)
                && Objects.equals(update, that.update)
                && Objects.equals(inferenceTimeout, that.inferenceTimeout)
                && Objects.equals(skipQueue, that.skipQueue)
                && Objects.equals(textInput, that.textInput);
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId, update, docs, inferenceTimeout, skipQueue, textInput);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, format("infer_trained_model_deployment[%s]", modelId), parentTaskId, headers);
        }

        public static class Builder {

            private String modelId;
            private List<Map<String, Object>> docs;
            private TimeValue timeout;
            private InferenceConfigUpdate update;
            private boolean skipQueue = false;
            private List<String> textInput;

            private Builder() {}

            public Builder setModelId(String modelId) {
                this.modelId = ExceptionsHelper.requireNonNull(modelId, InferModelAction.Request.MODEL_ID);
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

            private Builder setInferenceTimeout(String inferenceTimeout) {
                return setInferenceTimeout(TimeValue.parseTimeValue(inferenceTimeout, TIMEOUT.getPreferredName()));
            }

            public Builder setTextInput(List<String> textInput) {
                this.textInput = textInput;
                return this;
            }

            public Builder setSkipQueue(boolean skipQueue) {
                this.skipQueue = skipQueue;
                return this;
            }

            public Request build() {
                return new Request(modelId, update, docs, textInput, skipQueue, timeout);
            }
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private final List<InferenceResults> results;

        public Response(List<InferenceResults> results) {
            super(Collections.emptyList(), Collections.emptyList());
            this.results = Objects.requireNonNull(results);
        }

        public Response(StreamInput in) throws IOException {
            super(in);

            // Multiple results added in 8.6.1
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_6_1)) {
                results = in.readNamedWriteableList(InferenceResults.class);
            } else {
                results = List.of(in.readNamedWriteable(InferenceResults.class));
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);

            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_6_1)) {
                out.writeNamedWriteableList(results);
            } else {
                out.writeNamedWriteable(results.get(0));
            }
        }

        public List<InferenceResults> getResults() {
            return results;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            results.get(0).toXContent(builder, params);
            builder.endObject();
            return builder;
        }
    }
}
