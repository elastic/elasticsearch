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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.xpack.core.ml.action.InferModelAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CoordinatedInferenceAction extends ActionType<InferModelAction.Response> {

    public static final CoordinatedInferenceAction INSTANCE = new CoordinatedInferenceAction();
    public static final String NAME = "cluster:internal/xpack/coordinatedinference";

    public CoordinatedInferenceAction() {
        super(NAME, InferModelAction.Response::new);
    }

    public static class Request extends ActionRequest {

        public enum ModelHost {
            FOR_NLP_MODEL, // Either an inference service model or ml PyTorch model
            FOR_DFA_MODEL, // Boosted tree model
            UNKNOWN
        };

        public static Request forInferenceService(String modelId, List<String> inputs, @Nullable Map<String, Object> taskSettings) {
            return new Request(modelId, inputs, taskSettings, null, null, null, null, false);
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
                false // high priority
            );
        }

        public static Request forMapInput(
            String modelId,
            List<Map<String, Object>> objectsToInfer,
            @Nullable InferenceConfigUpdate inferenceConfigUpdate,
            @Nullable Boolean previouslyLicensed,
            @Nullable TimeValue inferenceTimeout
        ) {
            return new Request(
                modelId,
                null,
                null,
                objectsToInfer,
                inferenceConfigUpdate,
                previouslyLicensed,
                inferenceTimeout,
                false // high priority,
            );
        }

        private final String modelId;
        private ModelHost modelHost = ModelHost.UNKNOWN;
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


        private Request(
            String modelId,
            @Nullable List<String> inputs,
            @Nullable Map<String, Object> taskSettings,
            @Nullable List<Map<String, Object>> objectsToInfer,
            @Nullable InferenceConfigUpdate inferenceConfigUpdate,
            @Nullable Boolean previouslyLicensed,
            @Nullable TimeValue inferenceTimeout,
            boolean highPriority
        ) {
            this.modelId = ExceptionsHelper.requireNonNull(modelId, "model_id");
            this.inputs = inputs;
            this.taskSettings = taskSettings;
            this.objectsToInfer = objectsToInfer;
            this.inferenceConfigUpdate = inferenceConfigUpdate;
            this.previouslyLicensed = previouslyLicensed;
            this.inferenceTimeout = inferenceTimeout;
            this.highPriority = highPriority;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.modelId = in.readString();
            this.modelHost = in.readEnum(ModelHost.class);
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
            this.highPriority = in.readBoolean();
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

        public ModelHost getModelHost() {
            return modelHost;
        }

        public void setModelHost(ModelHost modelHost) {
            this.modelHost = modelHost;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelId);
            out.writeEnum(modelHost);
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
            out.writeBoolean(highPriority);
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
                && Objects.equals(modelHost, request.modelHost)
                && Objects.equals(inputs, request.inputs)
                && Objects.equals(taskSettings, request.taskSettings)
                && Objects.equals(objectsToInfer, request.objectsToInfer)
                && Objects.equals(inferenceConfigUpdate, request.inferenceConfigUpdate)
                && Objects.equals(previouslyLicensed, request.previouslyLicensed)
                && Objects.equals(inferenceTimeout, request.inferenceTimeout)
                && Objects.equals(highPriority, request.highPriority);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                modelId,
                modelHost,
                inputs,
                taskSettings,
                objectsToInfer,
                inferenceConfigUpdate,
                previouslyLicensed,
                inferenceTimeout,
                highPriority
            );
        }
    }
}
