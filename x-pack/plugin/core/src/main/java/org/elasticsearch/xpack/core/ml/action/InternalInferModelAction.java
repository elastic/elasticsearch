/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigUpdate;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalInferModelAction extends ActionType<InternalInferModelAction.Response> {

    public static final InternalInferModelAction INSTANCE = new InternalInferModelAction();
    public static final String NAME = "cluster:internal/xpack/ml/inference/infer";

    private InternalInferModelAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        private final String modelId;
        private final List<Map<String, Object>> objectsToInfer;
        private final InferenceConfigUpdate update;
        private final boolean previouslyLicensed;

        public Request(String modelId, boolean previouslyLicensed) {
            this(modelId, Collections.emptyList(), RegressionConfigUpdate.EMPTY_PARAMS, previouslyLicensed);
        }

        public Request(String modelId,
                       List<Map<String, Object>> objectsToInfer,
                       InferenceConfigUpdate inferenceConfig,
                       boolean previouslyLicensed) {
            this.modelId = ExceptionsHelper.requireNonNull(modelId, TrainedModelConfig.MODEL_ID);
            this.objectsToInfer = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(objectsToInfer, "objects_to_infer"));
            this.update = ExceptionsHelper.requireNonNull(inferenceConfig, "inference_config");
            this.previouslyLicensed = previouslyLicensed;
        }

        public Request(String modelId,
                       Map<String, Object> objectToInfer,
                       InferenceConfigUpdate update,
                       boolean previouslyLicensed) {
            this(modelId,
                Collections.singletonList(ExceptionsHelper.requireNonNull(objectToInfer, "objects_to_infer")),
                update,
                previouslyLicensed);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.modelId = in.readString();
            this.objectsToInfer = Collections.unmodifiableList(in.readList(StreamInput::readMap));
            if (in.getVersion().onOrAfter(Version.V_7_8_0)) {
                this.update = in.readNamedWriteable(InferenceConfigUpdate.class);
            } else {
                InferenceConfig oldConfig = in.readNamedWriteable(InferenceConfig.class);
                if (oldConfig instanceof RegressionConfig) {
                    this.update = RegressionConfigUpdate.fromConfig((RegressionConfig)oldConfig);
                } else if (oldConfig instanceof ClassificationConfig) {
                    this.update = ClassificationConfigUpdate.fromConfig((ClassificationConfig) oldConfig);
                } else {
                    throw new IOException("Unexpected configuration type [" + oldConfig.getName() + "]");
                }
            }
            this.previouslyLicensed = in.readBoolean();
        }

        public String getModelId() {
            return modelId;
        }

        public List<Map<String, Object>> getObjectsToInfer() {
            return objectsToInfer;
        }

        public InferenceConfigUpdate getUpdate() {
            return update;
        }

        public boolean isPreviouslyLicensed() {
            return previouslyLicensed;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelId);
            out.writeCollection(objectsToInfer, StreamOutput::writeMap);
            if (out.getVersion().onOrAfter(Version.V_7_8_0)) {
                out.writeNamedWriteable(update);
            } else {
                out.writeNamedWriteable(update.toConfig());
            }
            out.writeBoolean(previouslyLicensed);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InternalInferModelAction.Request that = (InternalInferModelAction.Request) o;
            return Objects.equals(modelId, that.modelId)
                && Objects.equals(update, that.update)
                && Objects.equals(previouslyLicensed, that.previouslyLicensed)
                && Objects.equals(objectsToInfer, that.objectsToInfer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId, objectsToInfer, update, previouslyLicensed);
        }

    }

    public static class Response extends ActionResponse {

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
            InternalInferModelAction.Response that = (InternalInferModelAction.Response) o;
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

        public static class Builder {
            private List<InferenceResults> inferenceResults;
            private String modelId;
            private boolean isLicensed;

            public Builder setInferenceResults(List<InferenceResults> inferenceResults) {
                this.inferenceResults = inferenceResults;
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
