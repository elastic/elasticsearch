/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig.MODEL_SIZE_BYTES;

public class PutTrainedModelAction extends ActionType<PutTrainedModelAction.Response> {

    public static final String DEFER_DEFINITION_DECOMPRESSION = "defer_definition_decompression";
    public static final PutTrainedModelAction INSTANCE = new PutTrainedModelAction();
    public static final String NAME = "cluster:admin/xpack/ml/inference/put";

    private PutTrainedModelAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        public static Request parseRequest(
            String modelId,
            boolean deferDefinitionValidation,
            boolean waitForCompletion,
            XContentParser parser
        ) {
            TrainedModelConfig.Builder builder = TrainedModelConfig.STRICT_PARSER.apply(parser, null);

            if (builder.getModelId() == null) {
                builder.setModelId(modelId).build();
            } else if (Strings.isNullOrEmpty(modelId) == false && modelId.equals(builder.getModelId()) == false) {
                // If we have model_id in both URI and body, they must be identical
                throw new IllegalArgumentException(
                    Messages.getMessage(
                        Messages.INCONSISTENT_ID,
                        TrainedModelConfig.MODEL_ID.getPreferredName(),
                        builder.getModelId(),
                        modelId
                    )
                );
            }
            // Validations are done against the builder so we can build the full config object.
            // This allows us to not worry about serializing a builder class between nodes.
            return new Request(builder.validate(true).build(), deferDefinitionValidation, waitForCompletion);
        }

        private final TrainedModelConfig config;
        private final boolean deferDefinitionDecompression;
        private final boolean waitForCompletion;

        // TODO: remove this constructor after re-factoring ML parts
        public Request(TrainedModelConfig config, boolean deferDefinitionDecompression) {
            this(config, deferDefinitionDecompression, false);
        }

        public Request(TrainedModelConfig config, boolean deferDefinitionDecompression, boolean waitForCompletion) {
            this.config = config;
            this.deferDefinitionDecompression = deferDefinitionDecompression;
            this.waitForCompletion = waitForCompletion;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.config = new TrainedModelConfig(in);
            this.deferDefinitionDecompression = in.readBoolean();
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
                this.waitForCompletion = in.readBoolean();
            } else {
                this.waitForCompletion = false;
            }
        }

        public TrainedModelConfig getTrainedModelConfig() {
            return config;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (deferDefinitionDecompression && config.getModelSize() == 0 && config.getCompressedDefinitionIfSet() != null) {
                ActionRequestValidationException validationException = new ActionRequestValidationException();
                validationException.addValidationError(
                    "when ["
                        + DEFER_DEFINITION_DECOMPRESSION
                        + "] is true and a compressed definition is provided, "
                        + MODEL_SIZE_BYTES
                        + " must be set"
                );
                return validationException;
            }
            return null;
        }

        public boolean isDeferDefinitionDecompression() {
            return deferDefinitionDecompression;
        }

        public boolean isWaitForCompletion() {
            return waitForCompletion;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            config.writeTo(out);
            out.writeBoolean(deferDefinitionDecompression);
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
                out.writeBoolean(waitForCompletion);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(config, request.config)
                && deferDefinitionDecompression == request.deferDefinitionDecompression
                && waitForCompletion == request.waitForCompletion;
        }

        @Override
        public int hashCode() {
            return Objects.hash(config, deferDefinitionDecompression, waitForCompletion);
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
