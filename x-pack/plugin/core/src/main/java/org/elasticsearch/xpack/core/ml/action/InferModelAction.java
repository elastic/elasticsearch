/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InferModelAction extends ActionType<InferModelAction.Response> {

    public static final InferModelAction INSTANCE = new InferModelAction();
    public static final String NAME = "cluster:admin/xpack/ml/inference/infer";

    private InferModelAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        private final String modelId;
        private final List<Map<String, Object>> objectsToInfer;
        private final InferenceConfig config;

        public Request(String modelId) {
            this(modelId, Collections.emptyList(), new RegressionConfig());
        }

        public Request(String modelId, List<Map<String, Object>> objectsToInfer, InferenceConfig inferenceConfig) {
            this.modelId = ExceptionsHelper.requireNonNull(modelId, TrainedModelConfig.MODEL_ID);
            this.objectsToInfer = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(objectsToInfer, "objects_to_infer"));
            this.config = ExceptionsHelper.requireNonNull(inferenceConfig, "inference_config");
        }

        public Request(String modelId, Map<String, Object> objectToInfer, InferenceConfig config) {
            this(modelId,
                Arrays.asList(ExceptionsHelper.requireNonNull(objectToInfer, "objects_to_infer")),
                config);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.modelId = in.readString();
            this.objectsToInfer = Collections.unmodifiableList(in.readList(StreamInput::readMap));
            this.config = in.readNamedWriteable(InferenceConfig.class);
        }

        public String getModelId() {
            return modelId;
        }

        public List<Map<String, Object>> getObjectsToInfer() {
            return objectsToInfer;
        }

        public InferenceConfig getConfig() {
            return config;
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
            out.writeNamedWriteable(config);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InferModelAction.Request that = (InferModelAction.Request) o;
            return Objects.equals(modelId, that.modelId)
                && Objects.equals(config, that.config)
                && Objects.equals(objectsToInfer, that.objectsToInfer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId, objectsToInfer, config);
        }

    }

    public static class Response extends ActionResponse {

        private final List<InferenceResults> inferenceResults;

        public Response(List<InferenceResults> inferenceResults) {
            super();
            this.inferenceResults = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(inferenceResults, "inferenceResults"));
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.inferenceResults = Collections.unmodifiableList(in.readNamedWriteableList(InferenceResults.class));
        }

        public List<InferenceResults> getInferenceResults() {
            return inferenceResults;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteableList(inferenceResults);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InferModelAction.Response that = (InferModelAction.Response) o;
            return Objects.equals(inferenceResults, that.inferenceResults);
        }

        @Override
        public int hashCode() {
            return Objects.hash(inferenceResults);
        }

    }
}
