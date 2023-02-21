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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.MODEL_ID;
import static org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.Request.NUMBER_OF_ALLOCATIONS;

public class UpdateTrainedModelDeploymentAction extends ActionType<CreateTrainedModelAssignmentAction.Response> {

    public static final UpdateTrainedModelDeploymentAction INSTANCE = new UpdateTrainedModelDeploymentAction();
    public static final String NAME = "cluster:admin/xpack/ml/trained_models/deployment/update";

    public UpdateTrainedModelDeploymentAction() {
        super(NAME, CreateTrainedModelAssignmentAction.Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        public static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        public static final ParseField TIMEOUT = new ParseField("timeout");

        static {
            PARSER.declareString(Request::setModelId, MODEL_ID);
            PARSER.declareInt(Request::setNumberOfAllocations, NUMBER_OF_ALLOCATIONS);
            PARSER.declareString((r, val) -> r.timeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
        }

        public static Request parseRequest(String modelId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (request.getModelId() == null) {
                request.setModelId(modelId);
            } else if (Strings.isNullOrEmpty(modelId) == false && modelId.equals(request.getModelId()) == false) {
                throw ExceptionsHelper.badRequestException(
                    Messages.getMessage(Messages.INCONSISTENT_ID, MODEL_ID, request.getModelId(), modelId)
                );
            }
            return request;
        }

        private String modelId;
        private int numberOfAllocations;

        private Request() {}

        public Request(String modelId) {
            setModelId(modelId);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            modelId = in.readString();
            numberOfAllocations = in.readVInt();
        }

        public final void setModelId(String modelId) {
            this.modelId = ExceptionsHelper.requireNonNull(modelId, MODEL_ID);
        }

        public String getModelId() {
            return modelId;
        }

        public void setNumberOfAllocations(int numberOfAllocations) {
            this.numberOfAllocations = numberOfAllocations;
        }

        public int getNumberOfAllocations() {
            return numberOfAllocations;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(modelId);
            out.writeVInt(numberOfAllocations);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MODEL_ID.getPreferredName(), modelId);
            builder.field(NUMBER_OF_ALLOCATIONS.getPreferredName(), numberOfAllocations);
            builder.endObject();
            return builder;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = new ActionRequestValidationException();
            if (numberOfAllocations < 1) {
                validationException.addValidationError("[" + NUMBER_OF_ALLOCATIONS + "] must be a positive integer");
            }
            return validationException.validationErrors().isEmpty() ? null : validationException;
        }

        @Override
        public int hashCode() {
            return Objects.hash(modelId, numberOfAllocations);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(modelId, other.modelId) && numberOfAllocations == other.numberOfAllocations;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
