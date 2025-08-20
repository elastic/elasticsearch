/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CoordinatedInferenceAction extends ActionType<InferModelAction.Response> {

    public static final CoordinatedInferenceAction INSTANCE = new CoordinatedInferenceAction();
    public static final String NAME = "cluster:internal/xpack/ml/coordinatedinference";

    public CoordinatedInferenceAction() {
        super(NAME);
    }

    public static class Request extends LegacyActionRequest {

        public enum RequestModelType {
            INFERENCE_SERVICE_MODEL,
            ML_NODE_PYTORCH_MODEL,
            BOOSTED_TREE_MODEL,
            NLP_MODEL,  // Either an inference service model or ml pytorch model but not a boosted tree model
            UNKNOWN
        }

        public static Request forTextInput(
            String modelId,
            List<String> inputs,
            @Nullable InferenceConfigUpdate inferenceConfigUpdate,
            @Nullable Boolean previouslyLicensed,
            @Nullable TimeValue inferenceTimeout
        ) {
            return new Request(
                modelId,
                inputs,
                null,
                null,
                inferenceConfigUpdate,
                previouslyLicensed,
                inferenceTimeout,
                false, // not high priority
                RequestModelType.NLP_MODEL
            );
        }

        public static Request forMapInput(
            String modelId,
            List<Map<String, Object>> objectsToInfer,
            @Nullable InferenceConfigUpdate inferenceConfigUpdate,
            @Nullable Boolean previouslyLicensed,
            @Nullable TimeValue inferenceTimeout,
            RequestModelType modelType
        ) {
            return new Request(
                modelId,
                null,
                null,
                objectsToInfer,
                inferenceConfigUpdate,
                previouslyLicensed,
                inferenceTimeout,
                false, // not high priority,
                modelType
            );
        }

        private final String modelId;
        private final RequestModelType requestModelType;
        // For inference services or cluster hosted NLP models
        private final List<String> inputs;
        // _inference settings
        private final Map<String, Object> taskSettings;
        // In cluster model options
        private final TimeValue inferenceTimeout;
        private final Boolean previouslyLicensed;
        private final InferenceConfigUpdate inferenceConfigUpdate;
        private boolean highPriority;
        private TrainedModelPrefixStrings.PrefixType prefixType = TrainedModelPrefixStrings.PrefixType.NONE;
        // DFA models only
        private final List<Map<String, Object>> objectsToInfer;

        // default for testing
        Request(
            String modelId,
            @Nullable List<String> inputs,
            @Nullable Map<String, Object> taskSettings,
            @Nullable List<Map<String, Object>> objectsToInfer,
            @Nullable InferenceConfigUpdate inferenceConfigUpdate,
            @Nullable Boolean previouslyLicensed,
            @Nullable TimeValue inferenceTimeout,
            boolean highPriority,
            RequestModelType requestModelType
        ) {
            this.modelId = ExceptionsHelper.requireNonNull(modelId, "model_id");
            this.inputs = inputs;
            this.taskSettings = taskSettings;
            this.objectsToInfer = objectsToInfer;
            this.inferenceConfigUpdate = inferenceConfigUpdate;
            this.previouslyLicensed = previouslyLicensed;
            this.inferenceTimeout = inferenceTimeout;
            this.highPriority = highPriority;
            this.requestModelType = requestModelType;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.modelId = in.readString();
            this.requestModelType = in.readEnum(RequestModelType.class);
            this.inputs = in.readOptionalStringCollectionAsList();
            this.taskSettings = in.readGenericMap();
            this.objectsToInfer = in.readOptionalCollectionAsList(StreamInput::readGenericMap);
            this.inferenceConfigUpdate = in.readOptionalNamedWriteable(InferenceConfigUpdate.class);
            this.previouslyLicensed = in.readOptionalBoolean();
            this.inferenceTimeout = in.readOptionalTimeValue();
            this.highPriority = in.readBoolean();
            // The prefixType was added prior to TransportVersions.V_8_13_0 but we're serializing it now
            // as a safety measure. At the time of writing this it doesn't have to be serialized because this class is only used internally
            // and on a single node so it never actually gets serialized. But we'll do it just in case that changes in the future.
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
                this.prefixType = in.readEnum(TrainedModelPrefixStrings.PrefixType.class);
            }
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

        public boolean getHighPriority() {
            return highPriority;
        }

        public void setHighPriority(boolean highPriority) {
            this.highPriority = highPriority;
        }

        public boolean hasInferenceConfig() {
            return inferenceConfigUpdate != null;
        }

        public boolean hasObjects() {
            return objectsToInfer != null;
        }

        public void setPrefixType(TrainedModelPrefixStrings.PrefixType prefixType) {
            this.prefixType = prefixType;
        }

        public TrainedModelPrefixStrings.PrefixType getPrefixType() {
            return prefixType;
        }

        public RequestModelType getRequestModelType() {
            return requestModelType;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelId);
            out.writeEnum(requestModelType);
            out.writeOptionalStringCollection(inputs);
            out.writeGenericMap(taskSettings);
            out.writeOptionalCollection(objectsToInfer, StreamOutput::writeGenericMap);
            out.writeOptionalNamedWriteable(inferenceConfigUpdate);
            out.writeOptionalBoolean(previouslyLicensed);
            out.writeOptionalTimeValue(inferenceTimeout);
            out.writeBoolean(highPriority);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_13_0)) {
                out.writeEnum(prefixType);
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(modelId, request.modelId)
                && Objects.equals(requestModelType, request.requestModelType)
                && Objects.equals(inputs, request.inputs)
                && Objects.equals(taskSettings, request.taskSettings)
                && Objects.equals(objectsToInfer, request.objectsToInfer)
                && Objects.equals(inferenceConfigUpdate, request.inferenceConfigUpdate)
                && Objects.equals(previouslyLicensed, request.previouslyLicensed)
                && Objects.equals(inferenceTimeout, request.inferenceTimeout)
                && Objects.equals(highPriority, request.highPriority)
                && Objects.equals(prefixType, request.prefixType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                modelId,
                requestModelType,
                inputs,
                taskSettings,
                objectsToInfer,
                inferenceConfigUpdate,
                previouslyLicensed,
                inferenceTimeout,
                highPriority,
                prefixType
            );
        }
    }
}
