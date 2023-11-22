/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CoordinatedInferenceAction extends ActionType<CoordinatedInferenceAction.Response> {

    public static final CoordinatedInferenceAction INSTANCE = new CoordinatedInferenceAction();
    public static final String NAME = "cluster:internal/xpack/coordinatedinference";

    public CoordinatedInferenceAction() {
        super(NAME, CoordinatedInferenceAction.Response::new);
    }

    public static class Request extends ActionRequest {

        public enum RequestType {
            FOR_INFERENCE_SERVICE,
            FOR_IN_CLUSTER_MODEL,
            FOR_DFA_MODEL
        }

        public static Request forInferenceService(String modelId, List<String> inputs, @Nullable Map<String, Object> taskSettings) {
            return new Request(modelId, inputs, taskSettings, null, null, null, null, null, RequestType.FOR_INFERENCE_SERVICE);
        }

        public static Request forInClusterInference(
            String modelId,
            List<String> inputs,
            @Nullable InferenceConfigUpdate inferenceConfigUpdate,
            @Nullable Boolean previouslyLicensed,
            @Nullable TimeValue inferenceTimeout,
            @Nullable Boolean highPriority
        ) {
            return new Request(
                modelId,
                inputs,
                null,
                null,
                inferenceConfigUpdate,
                previouslyLicensed,
                inferenceTimeout,
                highPriority,
                RequestType.FOR_IN_CLUSTER_MODEL
            );
        }

        public static Request forDFA(
            String modelId,
            List<Map<String, Object>> objectsToInfer,
            @Nullable InferenceConfigUpdate inferenceConfigUpdate,
            @Nullable Boolean previouslyLicensed,
            @Nullable TimeValue inferenceTimeout,
            @Nullable Boolean highPriority
        ) {
            return new Request(
                modelId,
                null,
                null,
                objectsToInfer,
                inferenceConfigUpdate,
                previouslyLicensed,
                inferenceTimeout,
                highPriority,
                RequestType.FOR_DFA_MODEL
            );
        }

        private final String modelId;
        // For inference services or cluster hosted NLP models
        private final List<String> inputs;
        // _inference settings
        private final Map<String, Object> taskSettings;
        // In cluster model options
        private final InferenceConfigUpdate inferenceConfigUpdate;
        private final Boolean previouslyLicensed;
        private final TimeValue inferenceTimeout;
        private final Boolean highPriority;
        // DFA models only
        private final List<Map<String, Object>> objectsToInfer;
        private final RequestType requestType;

        private Request(
            String modelId,
            @Nullable List<String> inputs,
            @Nullable Map<String, Object> taskSettings,
            @Nullable List<Map<String, Object>> objectsToInfer,
            @Nullable InferenceConfigUpdate inferenceConfigUpdate,
            @Nullable Boolean previouslyLicensed,
            @Nullable TimeValue inferenceTimeout,
            @Nullable Boolean highPriority,
            RequestType requestType
        ) {
            this.modelId = ExceptionsHelper.requireNonNull(modelId, "model_id");
            this.inputs = inputs;
            this.taskSettings = taskSettings;
            this.objectsToInfer = objectsToInfer;
            this.inferenceConfigUpdate = inferenceConfigUpdate;
            this.previouslyLicensed = previouslyLicensed;
            this.inferenceTimeout = inferenceTimeout;
            this.highPriority = highPriority;
            this.requestType = requestType;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.modelId = in.readString();
            this.inputs = in.readOptionalStringCollectionAsList();
            if (in.readBoolean()) {
                this.taskSettings = in.readMap();
            } else {
                this.taskSettings = null;
            }
            this.objectsToInfer = in.readOptionalCollectionAsList(StreamInput::readMap);
            this.inferenceConfigUpdate = in.readOptionalNamedWriteable(InferenceConfigUpdate.class);
            this.previouslyLicensed = in.readOptionalBoolean();
            this.inferenceTimeout = in.readOptionalTimeValue();
            this.highPriority = in.readOptionalBoolean();
            this.requestType = in.readEnum(RequestType.class);
        }

        public String getModelId() {
            return modelId;
        }

        public List<String> getInputs() {
            return inputs;
        }

        public Map<String, Object> getTaskSettings() {
            return taskSettings;
        }

        public List<Map<String, Object>> getObjectsToInfer() {
            return objectsToInfer;
        }

        public InferenceConfigUpdate getInferenceConfigUpdate() {
            return inferenceConfigUpdate;
        }

        public Boolean getPreviouslyLicensed() {
            return previouslyLicensed;
        }

        public TimeValue getInferenceTimeout() {
            return inferenceTimeout;
        }

        public Boolean getHighPriority() {
            return highPriority;
        }

        public RequestType getRequestType() {
            return requestType;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelId);
            out.writeOptionalStringCollection(inputs);
            boolean taskSettingsPresent = taskSettings != null;
            out.writeBoolean(taskSettingsPresent);
            if (taskSettingsPresent) {
                out.writeGenericMap(taskSettings);
            }
            out.writeOptionalCollection(objectsToInfer, StreamOutput::writeGenericMap);
            out.writeOptionalNamedWriteable(inferenceConfigUpdate);
            out.writeOptionalBoolean(previouslyLicensed);
            out.writeOptionalTimeValue(inferenceTimeout);
            out.writeOptionalBoolean(highPriority);
            out.writeEnum(requestType);
        }

        @Override
        public ActionRequestValidationException validate() {
            if (inputs != null && inputs.size() > 1) {
                // TODO support multiple inputs to _inference
                var ex = new ActionRequestValidationException();
                ex.addValidationError("Only a single input is supported");
                return ex;
            }
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(modelId, request.modelId)
                && Objects.equals(inputs, request.inputs)
                && Objects.equals(taskSettings, request.taskSettings)
                && Objects.equals(objectsToInfer, request.objectsToInfer)
                && Objects.equals(inferenceConfigUpdate, request.inferenceConfigUpdate)
                && Objects.equals(previouslyLicensed, request.previouslyLicensed)
                && Objects.equals(inferenceTimeout, request.inferenceTimeout)
                && Objects.equals(highPriority, request.highPriority)
                && Objects.equals(requestType, request.requestType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                modelId,
                inputs,
                taskSettings,
                objectsToInfer,
                inferenceConfigUpdate,
                previouslyLicensed,
                inferenceTimeout,
                highPriority,
                requestType
            );
        }
    }

    public static class Response extends ActionResponse {

        private final InferenceResults inferenceResult;

        public Response(InferenceResults inferenceResult) {
            this.inferenceResult = inferenceResult;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            inferenceResult = in.readOptionalNamedWriteable(InferenceResults.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalNamedWriteable(inferenceResult);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(inferenceResult, response.inferenceResult);
        }

        @Override
        public int hashCode() {
            return Objects.hash(inferenceResult);
        }
    }
}
