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
import org.elasticsearch.xpack.core.ml.inference.results.ClassificationInferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.results.RegressionInferenceResults;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InferModelAction extends ActionType<InferModelAction.Response> {

    public static final InferModelAction INSTANCE = new InferModelAction();
    public static final String NAME = "cluster:admin/xpack/ml/infer";

    private InferModelAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest {

        private final String modelId;
        private final long modelVersion;
        private final List<Map<String, Object>> objectsToInfer;
        private final int topClasses;

        public Request(String modelId, long modelVersion) {
            this(modelId, modelVersion, Collections.emptyList(), null);
        }

        public Request(String modelId, long modelVersion, List<Map<String, Object>> objectsToInfer, Integer topClasses) {
            this.modelId = ExceptionsHelper.requireNonNull(modelId, TrainedModelConfig.MODEL_ID);
            this.modelVersion = modelVersion;
            this.objectsToInfer = Collections.unmodifiableList(ExceptionsHelper.requireNonNull(objectsToInfer, "objects_to_infer"));
            this.topClasses = topClasses  == null ? 0 : topClasses;
        }

        public Request(String modelId, long modelVersion, Map<String, Object> objectToInfer, Integer topClasses) {
            this(modelId,
                modelVersion,
                Arrays.asList(ExceptionsHelper.requireNonNull(objectToInfer, "objects_to_infer")),
                topClasses);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.modelId = in.readString();
            this.modelVersion = in.readVLong();
            this.objectsToInfer = Collections.unmodifiableList(in.readList(StreamInput::readMap));
            this.topClasses = in.readInt();
        }

        public String getModelId() {
            return modelId;
        }

        public long getModelVersion() {
            return modelVersion;
        }

        public List<Map<String, Object>> getObjectsToInfer() {
            return objectsToInfer;
        }

        public int getTopClasses() {
            return topClasses;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelId);
            out.writeVLong(modelVersion);
            out.writeCollection(objectsToInfer, StreamOutput::writeMap);
            out.writeInt(topClasses);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InferModelAction.Request that = (InferModelAction.Request) o;
            return Objects.equals(modelId, that.modelId)
                && Objects.equals(modelVersion, that.modelVersion)
                && Objects.equals(topClasses, that.topClasses)
                && Objects.equals(objectsToInfer, that.objectsToInfer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId, modelVersion, objectsToInfer, topClasses);
        }

    }

    public static class Response extends ActionResponse {

        private final List<InferenceResults<?>> inferenceResults;
        private final String resultsType;

        public Response(List<InferenceResults<?>> inferenceResponse, String resultsType) {
            super();
            this.resultsType = ExceptionsHelper.requireNonNull(resultsType, "resultsType");
            this.inferenceResults = inferenceResponse == null ?
                Collections.emptyList() :
                Collections.unmodifiableList(inferenceResponse);
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            this.resultsType = in.readString();
            if(resultsType.equals(ClassificationInferenceResults.RESULT_TYPE)) {
                this.inferenceResults = Collections.unmodifiableList(in.readList(ClassificationInferenceResults::new));
            } else if (this.resultsType.equals(RegressionInferenceResults.RESULT_TYPE)) {
                this.inferenceResults = Collections.unmodifiableList(in.readList(RegressionInferenceResults::new));
            } else {
                throw new IOException("Unrecognized result type [" + resultsType + "]");
            }
        }

        public List<InferenceResults<?>> getInferenceResults() {
            return inferenceResults;
        }

        public String getResultsType() {
            return resultsType;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(resultsType);
            out.writeCollection(inferenceResults);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InferModelAction.Response that = (InferModelAction.Response) o;
            return Objects.equals(resultsType, that.resultsType) && Objects.equals(inferenceResults, that.inferenceResults);
        }

        @Override
        public int hashCode() {
            return Objects.hash(resultsType, inferenceResults);
        }

    }
}
