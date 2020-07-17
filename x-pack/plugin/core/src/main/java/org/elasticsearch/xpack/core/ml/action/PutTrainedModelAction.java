/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.io.IOException;
import java.util.Objects;


public class PutTrainedModelAction extends ActionType<PutTrainedModelAction.Response> {

    public static final PutTrainedModelAction INSTANCE = new PutTrainedModelAction();
    public static final String NAME = "cluster:admin/xpack/ml/inference/put";
    private PutTrainedModelAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        public static Request parseRequest(String modelId, XContentParser parser) {
            TrainedModelConfig.Builder builder = TrainedModelConfig.STRICT_PARSER.apply(parser, null);

            if (builder.getModelId() == null) {
                builder.setModelId(modelId).build();
            } else if (!Strings.isNullOrEmpty(modelId) && !modelId.equals(builder.getModelId())) {
                // If we have model_id in both URI and body, they must be identical
                throw new IllegalArgumentException(Messages.getMessage(Messages.INCONSISTENT_ID,
                    TrainedModelConfig.MODEL_ID.getPreferredName(),
                    builder.getModelId(),
                    modelId));
            }
            // Validations are done against the builder so we can build the full config object.
            // This allows us to not worry about serializing a builder class between nodes.
            return new Request(builder.validate(true).build());
        }

        private final TrainedModelConfig config;

        public Request(TrainedModelConfig config) {
            this.config = config;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.config = new TrainedModelConfig(in);
        }

        public TrainedModelConfig getTrainedModelConfig() {
            return config;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            config.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(config, request.config);
        }

        @Override
        public int hashCode() {
            return Objects.hash(config);
        }

        @Override
        public final String toString() {
            return Strings.toString(config);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final TrainedModelConfig trainedModelConfig;

        public Response(TrainedModelConfig trainedModelConfig) {
            this.trainedModelConfig = trainedModelConfig;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            trainedModelConfig = new TrainedModelConfig(in);
        }

        public TrainedModelConfig getResponse() {
            return trainedModelConfig;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            trainedModelConfig.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return trainedModelConfig.toXContent(builder, params);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(trainedModelConfig, response.trainedModelConfig);
        }

        @Override
        public int hashCode() {
            return Objects.hash(trainedModelConfig);
        }
    }
}
