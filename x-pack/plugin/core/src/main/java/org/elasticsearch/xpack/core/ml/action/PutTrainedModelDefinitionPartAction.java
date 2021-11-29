/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutTrainedModelDefinitionPartAction extends ActionType<AcknowledgedResponse> {
    public static final int MAX_NUM_NATIVE_DEFINITION_PARTS = 10_000;

    public static final PutTrainedModelDefinitionPartAction INSTANCE = new PutTrainedModelDefinitionPartAction();
    public static final String NAME = "cluster:admin/xpack/ml/trained_models/part/put";

    private PutTrainedModelDefinitionPartAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        public static final ParseField DEFINITION = new ParseField("definition");
        public static final ParseField TOTAL_DEFINITION_LENGTH = new ParseField("total_definition_length");
        public static final ParseField TOTAL_PARTS = new ParseField("total_parts");
        public static final String PART = "part";

        private static final ObjectParser<Request.Builder, Void> PARSER = new ObjectParser<>(
            "put_trained_model_part_action",
            Request.Builder::new
        );
        static {
            PARSER.declareField(Builder::setDefinition, XContentParser::binaryValue, DEFINITION, ObjectParser.ValueType.STRING);
            PARSER.declareLong(Builder::setTotalDefinitionLength, TOTAL_DEFINITION_LENGTH);
            PARSER.declareInt(Builder::setTotalParts, TOTAL_PARTS);
        }

        public static Request parseRequest(String modelId, int part, XContentParser parser) {
            return PARSER.apply(parser, null).build(modelId, part);
        }

        private final String modelId;
        private final BytesReference definition;
        private final int part;
        private final long totalDefinitionLength;
        private final int totalParts;

        public Request(String modelId, BytesReference definition, int part, long totalDefinitionLength, int totalParts) {
            this.modelId = ExceptionsHelper.requireNonNull(modelId, TrainedModelConfig.MODEL_ID);
            this.definition = ExceptionsHelper.requireNonNull(definition, DEFINITION);
            this.part = part;
            this.totalDefinitionLength = totalDefinitionLength;
            this.totalParts = totalParts;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.modelId = in.readString();
            this.definition = in.readBytesReference();
            this.part = in.readVInt();
            this.totalDefinitionLength = in.readVLong();
            this.totalParts = in.readVInt();
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (part < 0) {
                validationException = addValidationError("[part] must be greater or equal to 0", validationException);
            }
            if (totalParts <= 0) {
                validationException = addValidationError("[total_parts] must be greater than 0", validationException);
            }
            if (totalParts > MAX_NUM_NATIVE_DEFINITION_PARTS) {
                validationException = addValidationError(
                    "[total_parts] must be less than or equal to " + MAX_NUM_NATIVE_DEFINITION_PARTS,
                    validationException
                );
            }
            if (totalDefinitionLength <= 0) {
                validationException = addValidationError("[total_definition_length] must be greater than 0", validationException);
            }
            if (part >= totalParts) {
                validationException = addValidationError("[part] must be less than total_parts", validationException);
            }
            return validationException;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return part == request.part
                && totalDefinitionLength == request.totalDefinitionLength
                && totalParts == request.totalParts
                && Objects.equals(modelId, request.modelId)
                && Objects.equals(definition, request.definition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId, definition, part, totalDefinitionLength, totalParts);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelId);
            out.writeBytesReference(definition);
            out.writeVInt(part);
            out.writeVLong(totalDefinitionLength);
            out.writeVInt(totalParts);
        }

        public String getModelId() {
            return modelId;
        }

        public BytesReference getDefinition() {
            return definition;
        }

        public int getPart() {
            return part;
        }

        public long getTotalDefinitionLength() {
            return totalDefinitionLength;
        }

        public int getTotalParts() {
            return totalParts;
        }

        public static class Builder {
            private BytesReference definition;
            private long totalDefinitionLength;
            private int totalParts;

            public Builder setDefinition(byte[] definition) {
                this.definition = new BytesArray(definition);
                return this;
            }

            public Builder setTotalDefinitionLength(long totalDefinitionLength) {
                this.totalDefinitionLength = totalDefinitionLength;
                return this;
            }

            public Builder setTotalParts(int totalParts) {
                this.totalParts = totalParts;
                return this;
            }

            public Request build(String modelId, int part) {
                return new Request(modelId, definition, part, totalDefinitionLength, totalParts);
            }
        }
    }

}
